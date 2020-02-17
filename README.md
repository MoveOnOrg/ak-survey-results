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

## settings.py

Run `cp settings.py.example settings.py` and fill in any values needed for your environment.

## Command line usage

Run `python ak_survey_results.py --help` or check `settings.py.example` to get command line options. Command line options override settings.py.

## Amazon Lambda usage

Run `cp zappa_settings.json.example zappa_settings.json` and set any "[PICK-A-VALUE]" values as needed for your environment. Lambda event options override settings.py. And because Zappa can't currently configure CloudWatch to pass in event options directly, event.kwargs options also get translated to event options.

## GitHub Actions

Secrets are added to the repo as an encrypted tar file. When updating credentials, you'll need to recreate this file:

- `tar cvf secrets.tar zappa_settings.json settings.py test_settings.py`
- `openssl aes-256-cbc -pass "pass:ACTUAL-PASSWORD-HERE" -in secrets.tar -out secrets.tar.enc`

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
