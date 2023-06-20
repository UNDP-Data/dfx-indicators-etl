import os
import dotenv

dotenv.load_dotenv()

STANDARD_KEY_COLUMN = 'Alpha-3 code'
STANDARD_COUNTRY_COLUMN = 'Country'
COUNTRY_LOOKUP_CSV_PATH = "DataFuturePlatform/pipeline/config/utilities/country_lookup.xlsx"
AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
AZURE_STORAGE_CONTAINER_NAME = os.getenv('CONTAINER_NAME')
ROOT_FOLDER = os.getenv('ROOT_FOLDER')
