import os

from dotenv import load_dotenv

# Set the variables from the ENV file
load_dotenv()
RA_USER = os.getenv('RA_USER')
RA_PASS = os.getenv('RA_PASS')
RA_ID = os.getenv('RA_ID')
RA_NAME = os.getenv('RA_NAME')

# Download File details
primary_folder = '/Users/ryanmalesevich/Downloads/'
DOWNLOAD_FILE = primary_folder + RA_NAME + '.tab.zip'
EXTRACT_FOLDER = primary_folder + 'runningahead-logs/'

# Database details
DATABASE_NAME = '/Users/ryanmalesevich/Developer/runningahead-analyzer/runningahead-log.db'