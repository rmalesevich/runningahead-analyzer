# RunningAHEAD Analyzer

## runningahead-etl.py

Ensure the Chrome webdriver is installed. Easiest method is through brew.

```sh
brew install --cask chromedriver
```

To configure the script, ensure venv is installed. Then run the following script:

```sh
python3 -m venv venv
source venv/bin/activate
```

Install pip requirements:

```sh
pip install -r requirements.txt
```

Copy the .env-example to a .env file and set the right parameters.

```sh
cp .env-example .env
```

- RA_USER= Username for the RunningAHEAD website.
- RA_PASS= Password for the RunningAHEAD website.
- RA_NAME= Username from your RunningAHEAD account.
- RA_ID= The GUID from your RunningAHEAD account. Get this from the URL after you are logged in.
- PRIMARY_FOLDER= The entire path to the folder where the ZIP file is downloaded from RA.
- DATABASE_FILE= The entire path of the DuckDB database file.
- LOG_FILE= The path where the log files will be saved.

## analysis.py

This script is provided to illustrate some of the queries that can be executed through the DuckDB database.

The database is persisted so it can be read in other scripts or other tools.
