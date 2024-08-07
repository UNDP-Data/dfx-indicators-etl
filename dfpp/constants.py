from datetime import datetime, timezone

STANDARD_KEY_COLUMN = "Alpha-3 code"
STANDARD_COUNTRY_COLUMN = "Country"
COUNTRY_LOOKUP_CSV_PATH = "config/utilities/country_lookup.xlsx"
POOL_COMMAND_TIMEOUT = 15 * 60  # seconds
POOL_MINSIZE = 3
POOL_MAXSIZE = 50
CONNECTION_TIMEOUT = 30
CURRENT_YEAR = datetime.now(timezone.utc).year
