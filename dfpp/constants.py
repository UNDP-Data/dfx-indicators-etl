import datetime

STANDARD_KEY_COLUMN = 'Alpha-3 code'
STANDARD_COUNTRY_COLUMN = 'Country'
COUNTRY_LOOKUP_CSV_PATH = "DataFuturePlatform/pipeline/config/utilities/country_lookup.xlsx"
SOURCE_CONFIG_ROOT_FOLDER = "DataFuturePlatform/pipeline/config/sources"
INDICATOR_CONFIG_ROOT_FOLDER = "DataFuturePlatform/pipeline/config/indicators"
OUTPUT_FOLDER = "DataFuturePlatform/pipeline/output"
POOL_COMMAND_TIMEOUT = 15 * 60  # seconds
POOL_MINSIZE = 3
POOL_MAXSIZE = 50
CONNECTION_TIMEOUT = 30
TODAY = datetime.date.today()
CURRENT_YEAR = TODAY.year
