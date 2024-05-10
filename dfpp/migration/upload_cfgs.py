import os
import asyncio
import logging
import pathlib
import glob
from configparser import ConfigParser

import pandas as pd
from dotenv import load_dotenv

from dfpp.common import cfg2dict, dict2cfg
from dfpp.run_transform import transform_sources, run_transformation_for_indicator
from dfpp.storage import StorageManager

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
load_dotenv()
ROOT_FOLDER = os.getenv("ROOT_FOLDER")


async def upload_source_cfgs(source_cfgs_path: pathlib.Path) -> None:
    async with StorageManager(
            connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
            container_name=os.getenv("AZURE_STORAGE_CONTAINER_NAME"),
            root_folder=os.getenv("ROOT_FOLDER")
    ) as storage_manager:
        for root, dirs, files in os.walk(source_cfgs_path):
            for file in files:
                if file.endswith(".cfg"):
                    logger.info(f"Uploading {file} to Azure Blob Storage")
                    await storage_manager.upload(
                        dst_path=os.path.join(ROOT_FOLDER, "config", "sources", root.split('/')[-1], file),
                        src_path=os.path.join(root, file),
                        data=file.encode("utf-8"),
                        content_type="text/plain"
                    )
                    logger.info(f"Uploaded {file} to Azure Blob Storage")


async def upload_indicator_cfgs(indicator_cfgs_path: pathlib.Path) -> None:
    async with StorageManager(
            connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
            container_name=os.getenv("AZURE_STORAGE_CONTAINER_NAME"),
            root_folder=os.getenv("ROOT_FOLDER")
    ) as storage_manager:
        for root, dirs, files in os.walk(indicator_cfgs_path):
            for file in files:
                if file.endswith(".cfg"):
                    logger.info(f"Uploading {file} to Azure Blob Storage")
                    await storage_manager.upload(
                        dst_path=os.path.join(ROOT_FOLDER, "config", "indicators", file),
                        src_path=os.path.join(root, file),
                        data=file.encode("utf-8"),
                        content_type="text/plain"
                    )
                    logger.info(f"Uploaded {file} to Azure Blob Storage")


def count_number_of_outputs(path):
    file_path = glob.glob(f"{path}/*.json")
    files = [file for file in file_path]
    return len(files)


async def create_indicator_source_mapping():
    async with StorageManager(
            connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
            container_name=os.getenv("AZURE_STORAGE_CONTAINER_NAME"),
            root_folder=os.getenv("ROOT_FOLDER")
    ) as storage_manager:

        indicator_cfgs = await storage_manager.get_indicators_cfg()
        indicator_source_list = []
        for indicator_cfg in indicator_cfgs:
            source_id = indicator_cfg['indicator']['source_id']
            if source_id == "WBANK_INFO_VA_ESTIMATE" or source_id == "WBANK_INFO_VA_RANK":
                continue
            source_cfg = await storage_manager.get_source_cfg(source_id=source_id)
            source_url = source_cfg['source']['url']
            try:
                print(f"Adding indicator {indicator_cfg['indicator']['indicator_id']} with source {source_id}")
                indicator_source_list.append({
                    'indicator': indicator_cfg['indicator']['indicator_id'],
                    'source': indicator_cfg['indicator']['source_id'],
                    'source_url': source_url
                })
            except Exception as e:
                logger.error(e)

        data = pd.DataFrame(indicator_source_list)
        data.to_csv("indicator_source_mapping.csv", index=False)
        return indicator_source_list


def correct_hdi_indicators():
    indicator_list_path = "/home/thuha/Documents/indicator_list.csv"
    local_indicator_cfg_path = "/home/thuha/Desktop/Group5/indicators"
    indicator_df = pd.read_csv(indicator_list_path)
    for index, row in indicator_df.iterrows():
        indicator_id = row['Indicator ID']
        indicator_source = row['Source ID']
        parser = ConfigParser(interpolation=None)
        parser.read(f"{local_indicator_cfg_path}/{indicator_id}.cfg")
        indicator_dict = cfg2dict(parser)
        if indicator_dict.get('indicator') is None:
            print(f"Indicator {indicator_id} does not exist")
            continue
        indicator_dict['indicator']['source_id'] = indicator_source
        return_parser = dict2cfg(indicator_dict)
        with open(f"dict2cfg/{indicator_id}.cfg", "w") as f:
            return_parser.write(f)


def test_download():
    indicator_list_path = "/home/thuha/Documents/indicator_list.csv"
    sources_cfg_path = "/home/thuha/Desktop/Group5/sources"
    indicator_df = pd.read_csv(indicator_list_path)
    skip_sources = ["HDI", "VDEM", "GLOBAL_FINDEX_DATABASE"]
    for index, row in indicator_df.iterrows():
        indicator_id = row['Indicator ID']
        source_id = row['Source ID']
        parser = ConfigParser(interpolation=None)
        parser.read(f"{sources_cfg_path}/{source_id.lower()}/{source_id.lower()}.cfg")
        source_dict = cfg2dict(parser)
        # print(source_dict.get('source').get('id'))
        if source_dict.get('source').get('id') in skip_sources or 'ILO' in source_dict.get('source').get('id'):
            # print(f"Source {source_id} does not exist")
            continue
        source_url = source_dict['source']['url']
        download_function = source_dict['source']['downloader_function']
        print(f"downloading for indicator {indicator_id} with source {source_id}")
        download_cmd = f"python -m dfpp.cli run download -i {indicator_id}"
        os.system(download_cmd)
        print(f"processed for indicator {indicator_id} with source {source_id}")




async def copy_raw_sources(source_path=None, dst_path=None):
    async with StorageManager(
            connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
            container_name=os.getenv("AZURE_STORAGE_CONTAINER_NAME"),
            root_folder="DataFuturePlatform/"
    ) as storage_manager:
        for file in file_names:
            raw_path = storage_manager.ROOT_FOLDER + "Sources/Raw/" + file
            # download
            await storage_manager.download(blob_name=raw_path,
                                           dst_path=f"/home/thuha/Desktop/Group5/sources/raw/{file}")

            try:
                # upload
                await storage_manager.upload(
                    dst_path=storage_manager.SOURCES_PATH,
                    src_path=f"/home/thuha/Desktop/Group5/sources/raw/{file}",
                    data=file.encode("utf-8"),
                    content_type="text/plain"
                )
            except Exception as e:
                logger.error(e)
                continue



async def get_untransformed_indicators():
    async with StorageManager(
            connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
            container_name=os.getenv("AZURE_STORAGE_CONTAINER_NAME"),
            root_folder=os.getenv("ROOT_FOLDER")
    ) as storage_manager:
        print(len(transformed_indicators))
        indicator_cfgs = await storage_manager.get_indicators_cfg()
        untransformed_indicators = []
        for indicator_cfg in indicator_cfgs:
            indicator_id = indicator_cfg['indicator']['indicator_id']
            if indicator_id not in transformed_indicators:
                # source = indicator_cfg['indicator']['source_id']
                # source_cfg = await storage_manager.get_source_cfg(source_id=source)
                # preprocessing_function = source_cfg['source']['preprocessing_function']
                untransformed_indicators.append({
                    'indicator': indicator_cfg['indicator']['indicator_id'],
                    'source': indicator_cfg['indicator']['source_id'],
                    'preprocessing_function': indicator_cfg['indicator']['preprocessing']
                })
        data = pd.DataFrame(untransformed_indicators)
        # drop duplicates in the preprocessing function and keep the first one
        # data.drop_duplicates(subset=['preprocessing_function'], keep='first', inplace=True)
        data.to_csv("untransformed_indicators.csv", index=False)
        return untransformed_indicators


async def list_sources_with_NONE_in_file_format():
    async with StorageManager(
            connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
            container_name=os.getenv("AZURE_STORAGE_CONTAINER_NAME"),
            root_folder=os.getenv("ROOT_FOLDER")
    ) as storage_manager:
        source_cfgs = await storage_manager.get_sources_cfgs()
        sources_with_none = []
        for source_cfg in source_cfgs:
            if source_cfg['source']['file_format'] == "None":
                sources_with_none.append(source_cfg['source']['id'])
        return sources_with_none


async def run_transform_for_vdem_and_hdi_indicators():
    async with StorageManager(
            connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
            container_name=os.getenv("AZURE_STORAGE_CONTAINER_NAME"),
            root_folder=os.getenv("ROOT_FOLDER")
    ) as storage_manager:
        indicator_cfgs = await storage_manager.get_indicators_cfg()

        for indicator_cfg in indicator_cfgs:
            print("processing indicator: ", indicator_cfg.get('indicator').get('indicator_id'))
            await run_transformation_for_indicator(indicator_cfg=indicator_cfg.get('indicator'),
                                                   project='access_all_data')


async def replace_sources_with_string(search_string="https://www.ilo.org/sdmx/rest/data/", replace_string="https://sdmx.ilo.org/rest/data/"):
    async with StorageManager(
            connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
            container_name=os.getenv("AZURE_STORAGE_CONTAINER_NAME"),
            root_folder=os.getenv("ROOT_FOLDER")
    ) as storage_manager:
        source_cfgs = await storage_manager.get_sources_cfgs()
        for source_cfg in source_cfgs:
            source_id = source_cfg['source']['id']
            source_cfg['source']['url'] = source_cfg['source']['url'].replace(search_string, replace_string)
            return_parser = dict2cfg(source_cfg)
            with open(f"/home/thuha/Desktop/Group5/sources/{source_id.lower()}/{source_id.lower()}.cfg", "w") as f:
                return_parser.write(f)


if __name__ == "__main__":
    # asyncio.run(replace_sources_with_string())
    asyncio.run(upload_source_cfgs(source_cfgs_path=pathlib.Path("/home/thuha/Desktop/Group5/sources")))
    asyncio.run(upload_indicator_cfgs(
        indicator_cfgs_path=pathlib.Path("/home/thuha/Desktop/Group5/indicators")))
    # test_download()
    # asyncio.run(copy_raw_sources())
    # untransformed = asyncio.run(get_untransformed_indicators())
    # print(untransformed)
    # run()
    # sources = asyncio.run(list_sources_with_NONE_in_file_format())
    # print(sources)
    # from dotenv import load_dotenv
    # load_dotenv()

