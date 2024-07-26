import ast
import asyncio
import configparser
import json
import logging

import toml

from dfpp.download import country_downloader
from dfpp.storage import StorageManager, cfg2dict

logging.basicConfig()
logger = logging.getLogger()
logging_stream_handler = logging.StreamHandler()
logging_stream_handler.setFormatter(
    logging.Formatter(
        "%(asctime)s-%(filename)s:%(funcName)s:%(lineno)d:%(levelname)s:%(message)s",
        "%Y-%m-%d %H:%M:%S",
    )
)
logger.setLevel(logging.INFO)
logger.handlers.clear()
logger.addHandler(logging_stream_handler)
azlogger = logging.getLogger('azure.core.pipeline.policies.http_logging_policy')
azlogger.setLevel(logging.WARNING)
def recurse(d):
    print(f'parsing d')
    try:
        if isinstance(d, dict):
            loaded_d = d
        else:
            loaded_d = json.loads(d)
        for k, v in loaded_d.items():
            loaded_d[k] = recurse(v)
    except (json.JSONDecodeError, TypeError):
        print(f'd failed {d} {type(d)}')
        return d
    return loaded_d
def cfg2dict(config_object=None, c=None):
    """
    Copnverts a config object to dict
    :param config_object:
    :return: dict
    """
    print(config_object,c )
    output_dict = {}
    sections = config_object.sections()
    for section in sections:
        items = config_object.items(section)

        pitems = []
        for k, v in items:
            pitems.append((k, recurse(v)))
            # try:
            #     jv = json.loads(v,)
            #     pitems.append((k, jv))
            # except json.JSONDecodeError:
            #     pitems.append((k, v))

        output_dict[section] = pitems
        #print('haha', items)
    return output_dict
async def run():
    async with StorageManager() as sm:
        an_ind = await sm.get_indicator_cfg(indicator_id='expected_years_of_schooling_female_hdr')

        source_id = an_ind['indicator']['source_id']
        #source_cfg = await sm.get_source_cfg(source_id=source_id)

        with open('/home/janf/Downloads/data/dfpp/hdr.cfg') as src:
            content = src.read()
            parser = configparser.ConfigParser()
            parser.read_string(content)
            cfg_dict = cfg2dict(parser)
            print(json.dumps(cfg_dict, indent=2))

        '''
        source_id (str): A unique identifier for the data source.
        source_url (str): The base URL of the source from which to download country data.
        params_type (str): The type of operation to perform on the country codes DataFrame. Default is None.
        params_url (str): The URL within the parameters
        params_codes (str): A pipe-separated string of country codes to use as parameters
        '''
        #r = await country_downloader(source_id=source_id,source_url=)

async def inspect():
    async with StorageManager(clear_cache=True) as sm:
        indicators_cfg = await sm.get_indicators_cfg()
        for indicator_cfg in indicators_cfg:
            indicator_id = indicator_cfg['indicator']['indicator_id']
            source_id = indicator_cfg['indicator']['source_id']
            source_cfg = await sm.get_source_cfg(source_id=source_id)
            if source_cfg:
                if 'downloader_params' in source_cfg and source_cfg['downloader_params'] :
                    downloader_params = source_cfg['downloader_params']
                    if 'request_params' in downloader_params:
                        params_str = json.loads(source_cfg["downloader_params"]['request_params'])['value']
                        params = ast.literal_eval(params_str)
                        logger.info(f'{indicator_id}, {json.dumps(params, indent=2)}')
                # else:
                #     logger.info(f'{indicator_id} does not contain downloader params or is empty')
            else:
                logger.info(f'{indicator_id} does not have a source cfg')
asyncio.run(inspect())


