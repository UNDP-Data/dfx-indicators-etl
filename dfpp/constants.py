import os
from datetime import datetime, timezone

ROOT_FOLDER = os.getenv("ROOT_FOLDER")
STANDARD_KEY_COLUMN = "Alpha-3 code"
STANDARD_COUNTRY_COLUMN = "Country"
COUNTRY_LOOKUP_CSV_PATH = f"{ROOT_FOLDER}/config/utilities/country_lookup.xlsx"
SOURCE_CONFIG_ROOT_FOLDER = f"{ROOT_FOLDER}/config/sources"
INDICATOR_CONFIG_ROOT_FOLDER = f"{ROOT_FOLDER}/config/indicators"
OUTPUT_FOLDER = f"{ROOT_FOLDER}/output"
POOL_COMMAND_TIMEOUT = 15 * 60  # seconds
POOL_MINSIZE = 3
POOL_MAXSIZE = 50
CONNECTION_TIMEOUT = 30
CURRENT_YEAR = datetime.now(timezone.utc).year
