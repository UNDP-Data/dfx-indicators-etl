"""
A module responsible for publishing Data Futures Platform pipeline data sets

"""
import logging
from datetime import datetime


logger = logging.getLogger(__name__)
project = 'VaccineEquity'
output_data_type='LatestAvailableData'

def to_millis(d_str):
    if d_str == "-" or d_str == None or d_str == 'None':
        return 0
    return datetime.strptime(d_str, '%m/%d/%Y, %H:%M:%S').timestamp()




async def publish():
    logger.debug(f'Starting publish stage')
