# AK Survey Results

This Python 3 script will read recent survey results from an ActionKit database and save each survey to a table where each field is a column and each action is a row, for simpler, faster querying.

## Design Goals

This is designed to handle failure gracefully. If processing of a survey fails, other surveys should continue to update. If a table schema no longer matches the fields of incoming actions, the table should be updated. If the script stops running for a week and is turned back on, it should pick up where it left off and quickly catch up.

## Requirements

- Python 3
- A database with:
  - ActionKit tables (specifically `core_page`, `core_action`, and `core_actionfield`).
  - A table named `pages` with `page_id`, `column_list`, and `last_refresh` columns.
  - Access to create additional tables, named `page_{page_id}`.

ActionKit tables can be in a separate schema, but currently must be accessible within the same database connection.

## Settings

Settings are fetched directly from AWS Secrets Manager using the secret name `ak-survey-results`. 

## Command line usage

Run `python ak_survey_results.py --help` or check `settings.py.example` to get command line options. Command line options override settings.py.

## Amazon Lambda usage

Run `cp zappa_settings.json.example zappa_settings.json` and set any "[PICK-A-VALUE]" values as needed for your environment. Lambda event options override settings.py. And because Zappa can't currently configure CloudWatch to pass in event options directly, event.kwargs options also get translated to event options.

## Tests

To run tests:

- `cp settings.py.example test_settings.py` and fill in values. *Note*: in the test context, `DB_SCHEMA_AK` and `DB_SCHEMA_SURVEY` will be created and destroyed, so should not refer to existing schemas.
- `pip install -U pytest`
- `pytest`

To check test coverage:

- `pip install coverage`
- `coverage run -m pytest -s`
- `coverage report -m --include ak_survey_results.py`

To check code style:

- `pip install pycodestyle`
- `pycodestyle ak_survey_results.py`
