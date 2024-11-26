import os
import logging

from dotenv import load_dotenv

# Set the variables from the ENV file
load_dotenv()
RA_USER = os.getenv('RA_USER')
RA_PASS = os.getenv('RA_PASS')
RA_ID = os.getenv('RA_ID')
RA_NAME = os.getenv('RA_NAME')
SECOND_LIMIT = 10

# Download File details
primary_folder = os.getenv('PRIMARY_FOLDER')
DOWNLOAD_FILE = primary_folder + RA_NAME + '.tab.zip'
EXTRACT_FOLDER = primary_folder + 'runningahead-logs/'

# Database details
DATABASE_NAME = os.getenv('DATABASE_FILE')

# Logging details
LOGGING_FILE = os.getenv('LOG_FILE')
LOGGING_LEVEL = logging.DEBUG
LOGGING_FORMAT ='%(asctime)s - %(levelname)s - %(message)s'
LOGGING_DATEFMT = '%Y-%m-%d %H:%M:%S'
LOGGING_MAXBYTES = 20000000
LOGGING_BACKUPCOUNT = 3