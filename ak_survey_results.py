import boto3
import datetime
from json import dumps

import psycopg2
import psycopg2.extras
from slugify.main import Slugify
from pywell.entry_points import get_settings

ARG_DEFINITIONS = {
    'DB_HOST': 'Database host IP or hostname',
    'DB_PORT': 'Database port number',
    'DB_USER': 'Database user',
    'DB_PASS': 'Database password',
    'DB_NAME': 'Database name',
    'DB_SCHEMA_AK': 'Database schema for ActionKit tables',
    'DB_SCHEMA_SURVEY': 'Database schema for survey results tables',
    'DB_TYPE': 'Database type: PostgreSQL or Redshift',
    'FUNCTION': ('Function to call, e.g. '
                 'survey_refresh_info, process_recent_actions_for_survey'),
    'PAGE_ID': ('Survey page ID for survey_refresh_info, '
                'process_recent_actions_for_survey'),
    'SINCE': 'Time to check since, for process_recent_actions_for_survey',
    'COLUMN_EXCLUDES': '(Optional) Comma-separated list of columns to exclude',
    'LAMBDA': ('(Optional) AWS Lambda function to process surveys. '
               'If not supplied, surveys are processed synchronously.')
}

settings = get_settings(ARG_DEFINITIONS, 'ak-survey-results')


class PageNotFoundException(Exception):
    '''Raise this when requested page is not found'''


class PageNotSurveyException(Exception):
    '''Raise this when requested page is not a survey'''


class PageNotLoadedException(Exception):
    '''Raise this when requested page is not yet loaded'''


class InvalidDbTypeException(Exception):
    '''Raise this when the specified database type is not handled'''


class AKSurveyResults:

    def __init__(self, settings):
        """
        Initialize settings.
        """
        self.settings = settings
        self.database = psycopg2.connect(
            host=self.settings.DB_HOST,
            port=self.settings.DB_PORT,
            user=self.settings.DB_USER,
            password=self.settings.DB_PASS,
            database=self.settings.DB_NAME
        )
        self.database_cursor = self.database.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
        )
        self.custom_slugify = Slugify(to_lower=True)
        self.custom_slugify.separator = '_'
        self.varchar_col_type = ''
        if self.settings.DB_TYPE.lower() == 'redshift':
            self.varchar_col_type = 'VARCHAR(MAX)'
        elif self.settings.DB_TYPE.lower() == 'postgresql':
            self.varchar_col_type = 'VARCHAR'
        else:
            raise InvalidDbTypeException('Database type %s not found.'
                                         % self.settings.DB_TYPE)

    def survey_refresh_info(self, page_id):
        """
        Check refresh info for a given survey page ID.
        """
        survey_info_query = """
        SELECT p.type,
               sr.page_id,
               sr.column_list,
               sr.last_refresh,
               COUNT(a.id) AS action_count
        FROM %s.core_page p
        LEFT JOIN %s.pages sr ON sr.page_id = p.id
        LEFT JOIN %s.core_action a ON a.page_id = p.id
        WHERE p.id = %d
        GROUP BY 1,2,3,4
        """ % (
            self.settings.DB_SCHEMA_AK,
            self.settings.DB_SCHEMA_SURVEY,
            self.settings.DB_SCHEMA_AK,
            int(page_id)
        )
        self.database_cursor.execute(survey_info_query)
        survey_info_result = list(self.database_cursor.fetchall())
        if len(survey_info_result) == 0:
            raise PageNotFoundException('Page %d not found.' % int(page_id))
        result = survey_info_result[0]
        if result.get('type', '') != 'Survey':
            raise PageNotSurveyException(
                'Page %d is not a survey.' % int(page_id)
            )
        if not result.get('page_id', False):
            raise PageNotLoadedException(
                'Results for survey %d have not yet been loaded.'
                % int(page_id)
            )
        saved_count_query = """
        SELECT COUNT(pa.action_id) AS saved_count
        FROM %s.page_%d pa
        """ % (
            self.settings.DB_SCHEMA_SURVEY,
            int(page_id)
        )
        self.database_cursor.execute(saved_count_query)
        saved_count_result = list(self.database_cursor.fetchall())
        result['saved_count'] = saved_count_result[0].get('saved_count', 0)
        return result

    def recent_actions_for_survey(self, page_id, since='1900-01-01 00:00:00'):
        """
        Get all actions for a given page ID (page_id) since the given
        time (since).
        """
        actions_query = """
        SELECT a.id, a.created_at
        FROM %s.core_action a
        WHERE a.page_id = %d
        AND a.created_at >= '%s'
        ORDER BY a.created_at ASC
        LIMIT 10000
        """ % (
            self.settings.DB_SCHEMA_AK,
            int(page_id),
            since
        )
        self.database_cursor.execute(actions_query)
        return list(self.database_cursor.fetchall())

    def max_created_at(self, actions):
        max_created_at = datetime.datetime(1900, 1, 1, 0, 0)
        for action in actions:
            created_at = action.get(
                'created_at',
                datetime.datetime(1900, 1, 1, 0, 0)
            )
            if created_at > max_created_at:
                max_created_at = created_at
        return max_created_at

    def field_values_for_actions(self, action_ids):
        """
        Get all field values for given list of action IDs.
        """
        excludes = self.settings.COLUMN_EXCLUDES.split(',')
        values_query = """
        SELECT af.parent_id AS action_id, af.name, af.value
        FROM %s.core_actionfield af
        WHERE af.parent_id IN (%s)
        AND NOT af.name = ANY(%s)
        """ % (
            self.settings.DB_SCHEMA_AK,
            ', '.join([str(id) for id in action_ids]),
            '%s'
        )
        self.database_cursor.execute(values_query, (excludes, ))
        return list(self.database_cursor.fetchall())

    def field_values_by_action(self, field_values, name_map, action_ids):
        by_action = {}
        for action_id in action_ids:
            if not by_action.get(action_id, False):
                by_action[action_id] = {}
        for field_value in field_values:
            action_id = field_value.get('action_id', 0)
            name = field_value.get('name', '')
            value = field_value.get('value', '')
            if not by_action.get(action_id, False):
                by_action[action_id] = {}
            if not by_action[action_id].get(name_map.get(name, ''), False):
                by_action[action_id][name_map.get(name, '')] = value
            else:
                by_action[action_id][name_map.get(name, '')] += '; %s' % value
        return by_action

    def deduped_columns(self, columns):
        deduped_columns = []
        reserved_keywords = [
            'union', 'permissions', 'select', 'else', 'when', 'where', 'order',
            'primary', 'identity', 'join'
        ]
        for column in columns:
            column_name = column
            # Replace emtpy column name with "unnamed_field"
            if column_name == '':
                column_name = 'unnamed_field'
            # Append _q to reserved keywords
            if column_name in reserved_keywords:
                column_name = column_name + '_q'
            # Prepend q_ to things that start with numbers
            if column_name[0].isdigit():
                column_name = 'q_' + column_name
            # Append count to duplicates
            count = deduped_columns.count(column_name)
            if count > 0:
                deduped_columns.append(column_name + str(count + 1))
            else:
                deduped_columns.append(column_name)
        return deduped_columns

    def unique_field_names(self, field_values):
        """
        Reduce a list of core_actionfield records to unique field names.
        """
        field_names = list(set([
            row.get('name') for row in field_values if row.get('name', False)
        ]))
        field_names.sort()
        return field_names

    def sluggified_field_names(self, field_value_names):
        """
        Sluggify a list of field names.
        """
        return self.deduped_columns(
            [self.custom_slugify(name) for name in field_value_names]
        )

    def survey_table_needs_recreating(self, page_id, column_list):
        """
        Determine whether a table structure should be recreated, if new column
        list includes anything not yet in saved column list.
        """
        refresh_info = self.survey_refresh_info(page_id)
        saved_columns = refresh_info.get('column_list', '').split(',')
        return len(list(set(column_list) - set(saved_columns))) > 0

    def column_list_for_survey(self, page_id):
        """
        Full column list for given survey page_id.
        """
        excludes = self.settings.COLUMN_EXCLUDES.split(',')
        column_query = """
        SELECT DISTINCT af.name
        FROM %s.core_actionfield af
        JOIN %s.core_action a ON a.id = af.parent_id
        WHERE a.page_id = %d
        AND NOT af.name = ANY(%s)
        """ % (
            self.settings.DB_SCHEMA_AK,
            self.settings.DB_SCHEMA_AK,
            int(page_id),
            '%s'
        )
        self.database_cursor.execute(column_query, (excludes, ))
        column_result = list(self.database_cursor.fetchall())
        field_names = [item.get('name', '') for item in column_result]
        field_names.sort()
        return self.sluggified_field_names(field_names)

    def recreate_survey_table(self, page_id, column_list):
        """
        Recreate a survey's table structure.
        """
        drop_query = """
        DROP TABLE IF EXISTS %s.page_%d
        """ % (self.settings.DB_SCHEMA_SURVEY, int(page_id))
        self.database_cursor.execute(drop_query)
        create_columns = ['%s %s' % (column, self.varchar_col_type)
                          for column in column_list]
        create_columns.insert(0, 'action_id INTEGER')
        create_query = """
        CREATE TABLE %s.page_%d (%s)
        """ % (
            self.settings.DB_SCHEMA_SURVEY,
            int(page_id),
            ', '.join(create_columns)
        )
        self.database_cursor.execute(create_query)
        self.database.commit()

    def insert_rows_from_field_values(
        self, page_id, field_values, column_names
    ):
        """
        Insert results into survey_results table.
        """
        insert_columns = ['action_id'] + column_names
        insert_values = []
        action_ids = []
        for action_id, values in field_values.items():
            insert_values.append(action_id)
            action_ids.append(action_id)
            for column in column_names:
                insert_values.append(values.get(column, ''))
        delete_query = """
        DELETE FROM %s.page_%d WHERE action_id = ANY(%s)
        """ % (
            self.settings.DB_SCHEMA_SURVEY,
            int(page_id),
            '%s'
        )
        self.database_cursor.execute(delete_query, (action_ids,))
        self.database.commit()
        insert_query = """
        INSERT INTO %s.page_%d (%s) VALUES %s
        """ % (
            self.settings.DB_SCHEMA_SURVEY,
            int(page_id),
            ', '.join(insert_columns),
            """,
            """.join([
                '(%s)' % ', '.join([
                    '%s' for column in insert_columns
                ]) for row in field_values.keys()
            ])
        )
        self.database_cursor.execute(insert_query, insert_values)
        self.database.commit()

    def add_to_pages_table(self, page_id, column_list):
        """
        Insert the record for a given survey page.
        """
        insert_query = """
        INSERT INTO %s.pages
        (page_id, column_list, last_refresh)
        VALUES
        (%d, '%s', '1900-01-01 00:00:00')
        """ % (
            self.settings.DB_SCHEMA_SURVEY,
            int(page_id),
            ','.join(column_list)
        )
        self.database_cursor.execute(insert_query)
        self.database.commit()

    def delete_from_pages_table(self, page_id):
        """
        Delete the record for a given survey page.
        """
        delete_query = """
        DELETE FROM %s.pages
        WHERE page_id = %d
        """ % (
            self.settings.DB_SCHEMA_SURVEY,
            int(page_id)
        )
        self.database_cursor.execute(delete_query)
        self.database.commit()

    def update_pages_table_refresh(self, page_id, last_refresh):
        """
        Update the record for a given survey page.
        """
        if last_refresh != 'GETDATE()':
            last_refresh = "'%s'" % last_refresh
        update_query = """
        UPDATE %s.pages
        SET last_refresh = %s
        WHERE page_id = %d
        """ % (
            self.settings.DB_SCHEMA_SURVEY,
            last_refresh,
            int(page_id)
        )
        self.database_cursor.execute(update_query)
        self.database.commit()

    def surveys_that_need_updating(self, count):
        """
        Get a list of survey page IDs that need updating.
        """
        needs_update_query = """
        SELECT
            DISTINCT p.id AS page_id,
            CASE
                WHEN sr.last_refresh IS NOT NULL
                THEN sr.last_refresh
                ELSE '1900-01-01 00:00:00'
            END AS since
        FROM %s.core_action a
        JOIN %s.core_page p ON p.id = a.page_id
        LEFT JOIN %s.pages sr ON sr.page_id = p.id
        WHERE p.type = 'Survey'
          AND (sr.page_id IS NULL
               OR sr.last_refresh < a.created_at)
        ORDER BY p.id DESC
        LIMIT %d
        """ % (
            self.settings.DB_SCHEMA_AK,
            self.settings.DB_SCHEMA_AK,
            self.settings.DB_SCHEMA_SURVEY,
            count
        )
        self.database_cursor.execute(needs_update_query)
        return list(self.database_cursor.fetchall())

    def process_recent_actions_for_survey(self, page_id, since):
        actions = self.recent_actions_for_survey(page_id, since)
        action_ids = [action.get('id') for action in actions]
        if len(actions):
            field_values = self.field_values_for_actions(action_ids)
            field_names = self.unique_field_names(field_values)
            field_slugs = self.sluggified_field_names(field_names)
            try:
                needs_recreating = self.survey_table_needs_recreating(
                    page_id, field_slugs
                )
                if needs_recreating:
                    all_columns = self.column_list_for_survey(page_id)
                    self.recreate_survey_table(page_id, all_columns)
                    self.delete_from_pages_table(page_id)
                    self.add_to_pages_table(page_id, all_columns)
                    return {
                        'outcome': 'survey schema updated',
                        'reason': 'column(s) added'
                    }
                else:
                    self.insert_rows_from_field_values(
                        page_id,
                        self.field_values_by_action(
                            field_values,
                            dict(zip(field_names, field_slugs)),
                            action_ids
                        ),
                        field_slugs
                    )
                    self.update_pages_table_refresh(
                        page_id,
                        self.max_created_at(actions)
                    )
                    return {
                        'outcome': 'processed',
                        'actions': len(actions)
                    }
            except PageNotLoadedException as e:
                all_columns = self.column_list_for_survey(page_id)
                self.recreate_survey_table(page_id, all_columns)
                self.add_to_pages_table(page_id, all_columns)
                return {
                    'outcome': 'survey schema updated',
                    'reason': 'survey schema missing'
                }
        else:
            self.update_pages_table_refresh(page_id, 'GETDATE()')

    def process_surveys_that_need_updating(self, surveys, lambda_name=False):
        """
        Call process_recent_actions_for_survey on each survey.
        """
        if lambda_name:
            client = boto3.client('lambda')
            for survey in surveys:
                payload = dumps({
                    'FUNCTION': 'process_recent_actions_for_survey',
                    'PAGE_ID': survey.get('page_id'),
                    'SINCE': survey.get('since')
                }, default=json_serial)
                client.invoke(
                    FunctionName=lambda_name,
                    InvocationType='Event',
                    Payload=payload
                )
            return {
                'outcome': 'processing asynchronously',
                'surveys': surveys
            }
        else:
            results = {}
            for survey in surveys:
                results[survey.get('page_id')] = \
                    self.process_recent_actions_for_survey(
                        survey.get('page_id'),
                        survey.get('since')
                    )
            return {
                'outcome': 'processed',
                'surveys': surveys,
                'results': results
            }


class Struct:
    def __init__(self, **entries):
        self.__dict__.update(entries)


def main(args):
    ak = AKSurveyResults(args)
    print('running main')
    print(args.__dict__.get('FUNCTION', ''), 'FUNCTION')
    print(args.__dict__.get('PAGE_ID', ''), 'PAGE_ID')
    print(args.__dict__.get('SINCE', ''), 'SINCE')
    if args.FUNCTION == 'survey_refresh_info':
        try:
            return ak.survey_refresh_info(args.PAGE_ID)
        except Exception as e:
            return {
                'error': str(e)
            }
    elif args.FUNCTION == 'surveys_that_need_updating':
        try:
            return ak.surveys_that_need_updating(15)
        except Exception as e:
            return {
                'error': str(e)
            }
    elif args.FUNCTION == 'process_surveys_that_need_updating':
        try:
            surveys = ak.surveys_that_need_updating(15)
            return ak.process_surveys_that_need_updating(surveys, args.LAMBDA)
        except Exception as e:
            return {
                'error': str(e)
            }
    elif args.FUNCTION == 'process_recent_actions_for_survey':
        try:
            return ak.process_recent_actions_for_survey(
                args.PAGE_ID,
                args.SINCE
            )
        except Exception as e:
            return {
                'error': str(e)
            }
    return False


def json_serial(obj):
    """JSON serializer for objects not serializable by default JSON code."""
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError("Type %s not serializable." % type(obj))


def aws_lambda(event, context):
    """
    General entry point via Amazon Lambda event.
    """
    print('running aws_lambda')
    kwargs = event.get('kwargs', False)
    if kwargs:
        for argname in kwargs:
            event[argname] = kwargs.get(argname)
    for argname, helptext in ARG_DEFINITIONS.items():
        if not event.get(argname, False):
            event[argname] = settings.get(argname, False)
    print(event.get('FUNCTION', ''), 'FUNCTION')
    print(event.get('PAGE_ID', ''), 'PAGE_ID')
    print(event.get('SINCE', ''), 'SINCE')
    args = Struct(**event)
    return dumps(main(args), default=json_serial)


if __name__ == '__main__':
    """
    Entry point via command line.
    """
    import argparse
    import pprint

    parser = argparse.ArgumentParser(
        description='Work with ActionKit survey results.'
    )
    pp = pprint.PrettyPrinter(indent=2)

    for argname, helptext in ARG_DEFINITIONS.items():
        parser.add_argument(
            '--%s' % argname, dest=argname, help=helptext,
            default=settings.get(argname, False)
        )

    args = parser.parse_args()
    pp.pprint(main(args))
