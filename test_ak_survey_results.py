import pytest
import boto3
import json
from moto import mock_secretsmanager
from pywell.secrets_manager import get_secret


mock_secret = {'DB_SCHEMA_AK': 'ak', 'DB_SCHEMA_SURVEY': 'survey_results', 'DB_TYPE': 'PostgreSQL', 'COLUMN_EXCLUDES': ''}
redshift_secret={'username':'postgres','password':'','host':'localhost','port':'5432', 'dbName': 'postgres'}

class ArgsObject:
    def __init__(self, dict_args):
        for arg, val in dict_args.items():
            setattr(self, arg, val)

@mock_secretsmanager
def init_secrets():
    client = boto3.client('secretsmanager')
    client.create_secret(Name='ak-survey-results', SecretString=json.dumps(mock_secret))
    client.create_secret(Name='redshift-admin', SecretString=json.dumps(redshift_secret))


@mock_secretsmanager
class Test:

    def setup_method(self, method):
        init_secrets()
        self.args = get_secret('ak-survey-results')
        self.args['FUNCTION']='survey_refresh_info'
        self.args['PAGE_ID']=1
        self.args['SINCE']=60

        from ak_survey_results import AKSurveyResults
        self.survey_results = AKSurveyResults(ArgsObject(self.args))

        create_survey_schema_query = """
        CREATE SCHEMA %s
        """ % self.args['DB_SCHEMA_SURVEY']
        self.survey_results.database_cursor.execute(create_survey_schema_query)

        create_pages_table_query = """
        CREATE TABLE %s.pages (
            page_id INTEGER,
            last_refresh TIMESTAMP,
            column_list %s
        )
        """ % (self.args['DB_SCHEMA_SURVEY'], self.survey_results.varchar_col_type)
        self.survey_results.database_cursor.execute(create_pages_table_query)

        create_ak_schema_query = """
        CREATE SCHEMA %s
        """ % self.args['DB_SCHEMA_AK']
        self.survey_results.database_cursor.execute(create_ak_schema_query)

        create_page_table_query = """
        CREATE TABLE %s.core_page (
            id INTEGER,
            type VARCHAR(765)
        )
        """ % self.args['DB_SCHEMA_AK']
        self.survey_results.database_cursor.execute(create_page_table_query)

        create_action_table_query = """
        CREATE TABLE %s.core_action (
            id INTEGER,
            page_id INTEGER,
            created_at TIMESTAMP
        )
        """ % self.args['DB_SCHEMA_AK']
        self.survey_results.database_cursor.execute(create_action_table_query)

        create_actionfield_table_query = """
        CREATE TABLE %s.core_actionfield (
            id INTEGER,
            parent_id INTEGER,
            name VARCHAR(765),
            value %s
        )
        """ % (self.args['DB_SCHEMA_AK'], self.survey_results.varchar_col_type)
        self.survey_results.database_cursor.execute(
            create_actionfield_table_query
        )

        page_query = """
        INSERT INTO %s.core_page (id, type)
        VALUES
        (1, 'Donation'), (2, 'Survey'),
        (3, 'Survey'), (4, 'Survey'), (5, 'Survey')
        """ % self.args['DB_SCHEMA_AK']
        self.survey_results.database_cursor.execute(page_query)

        action_query = """
        INSERT INTO %s.core_action (id, page_id, created_at)
        VALUES
        (1, 2, '2018-10-01 01:01:01'),
        (2, 3, '2018-10-05 01:01:01'),
        (3, 4, '2018-10-06 01:01:01')
        """ % self.args['DB_SCHEMA_AK']
        self.survey_results.database_cursor.execute(action_query)

        actionfield_query = """
        INSERT INTO %s.core_actionfield (id, parent_id, name, value)
        VALUES
        (1, 1, 'name', 'value'),
        (2, 2, 'another', 'a value'),
        (3, 3, 'processed', 'yes')
        """ % self.args['DB_SCHEMA_AK']
        self.survey_results.database_cursor.execute(actionfield_query)

        create_page_query = """
        CREATE TABLE %s.page_4 (
            action_id INTEGER,
            processed %s
        )
        """ % (self.args['DB_SCHEMA_SURVEY'], self.survey_results.varchar_col_type)
        self.survey_results.database_cursor.execute(create_page_query)

        process_page_query = """
        INSERT INTO %s.pages (page_id, last_refresh, column_list)
        VALUES (4, '2018-10-06 01:01:01', 'processed'),
        (5, '2018-10-06 01:01:01', 'processed')
        """ % self.args['DB_SCHEMA_SURVEY']
        self.survey_results.database_cursor.execute(process_page_query)

        process_page_action_query = """
        INSERT INTO %s.page_4 (action_id, processed)
        VALUES (3, 'yes')
        """ % self.args['DB_SCHEMA_SURVEY']
        self.survey_results.database_cursor.execute(process_page_action_query)

        self.survey_results.database.commit()

    def test_sluggified_field_names(self):
        field_names = ['a', 'b', 'WHERE', 'when', 'A']
        assert_names = ['a', 'b', 'where_q', 'when_q', 'a2']
        sluggified_names = self.survey_results.sluggified_field_names(
            field_names
        )
        assert len(sluggified_names) == len(assert_names)
        assert sluggified_names == assert_names

    def test_survey_refresh_info(self):
        from ak_survey_results import PageNotFoundException
        from ak_survey_results import PageNotSurveyException
        from ak_survey_results import PageNotLoadedException
        with pytest.raises(PageNotFoundException) as e:
            self.survey_results.survey_refresh_info(0)
        with pytest.raises(PageNotSurveyException) as e:
            self.survey_results.survey_refresh_info(1)
        with pytest.raises(PageNotLoadedException) as e:
            self.survey_results.survey_refresh_info(2)
        with pytest.raises(PageNotLoadedException):
            self.survey_results.survey_refresh_info(5)
        results = self.survey_results.survey_refresh_info(4)
        assert results.get('action_count', 0) == 1
        assert results.get('saved_count', 0) == 1

    def test_recent_actions_for_survey(self):
        actions = self.survey_results.recent_actions_for_survey(2)
        assert len(actions) == 1
        assert actions[0].get('id', 0) == 1
        no_actions = self.survey_results.recent_actions_for_survey(1)
        assert no_actions == []

    def test_surveys_that_need_updating(self):
        surveys = self.survey_results.surveys_that_need_updating(10)
        assert len(surveys) == 2
        survey_ids = sorted([survey.get('page_id') for survey in surveys])
        assert survey_ids == [2, 3]

    def test_process_recent_actions_for_survey(self):
        self.survey_results.process_recent_actions_for_survey(2, '1900-01-01 00:00:00')
        survey_status = self.survey_results.survey_refresh_info(2)
        assert survey_status.get('action_count', False) == 1
        assert survey_status.get('saved_count', False) == 0
        self.survey_results.process_recent_actions_for_survey(2, '1900-01-01 00:00:00')
        surveys = self.survey_results.surveys_that_need_updating(10)
        assert len(surveys) == 1
        survey_ids = sorted([survey.get('page_id') for survey in surveys])
        assert survey_ids == [3]

    def test_process_surveys_that_need_updating(self):
        surveys = self.survey_results.surveys_that_need_updating(10)
        self.survey_results.process_surveys_that_need_updating(surveys)
        survey_status = self.survey_results.survey_refresh_info(3)
        assert survey_status.get('action_count', False) == 1
        assert survey_status.get('saved_count', False) == 0
        self.survey_results.process_surveys_that_need_updating(surveys)
        surveys_after = self.survey_results.surveys_that_need_updating(10)
        assert surveys_after == []

    def teardown_method(self, method):
        drop_survey_schema_query = """
        DROP SCHEMA %s CASCADE
        """ % self.args['DB_SCHEMA_SURVEY']
        self.survey_results.database_cursor.execute(drop_survey_schema_query)

        drop_ak_schema_query = """
        DROP SCHEMA %s CASCADE
        """ % self.args['DB_SCHEMA_AK']
        self.survey_results.database_cursor.execute(drop_ak_schema_query)
        self.survey_results.database.commit()
