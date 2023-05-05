import ast
import io
import logging
import os
import shutil
import tempfile
import zipfile
from configparser import ConfigParser
from typing import Any, Optional, Tuple

import aiohttp
import azblob
import numpy as np
import pandas as pd
from aiohttp import ClientTimeout

from dfpp.storage import AzureBlobStorageManager

CONNECTION_STRING = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
CONTAINER_NAME = os.environ["CONTAINER_NAME"]
ROOT_FOLDER = os.environ["ROOT_FOLDER"]
DEFAULT_TIMEOUT = aiohttp.ClientTimeout(total=600)


async def simple_url_download(
    url: str, timeout: ClientTimeout = DEFAULT_TIMEOUT, max_retries: int = 5
) -> Optional[Tuple[bytes, str]]:
    """
    Downloads the content in bytes from a given URL using the aiohttp library.

    :param url: The URL to download.
    :param timeout: The maximum amount of time to wait for a response from the server, in seconds.
    :param max_retries: The maximum number of times to retry the download if an error occurs.
    :return: a tuple containing the downloaded content and the content type, or None if the download fails.
    """

    for retry_count in range(max_retries):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=timeout) as resp:
                    if resp.status == 200:
                        downloaded_bytes = 0
                        chunk_size = 1024
                        data = b""
                        logger.info(f"Downloading {url}")
                        async for chunk in resp.content.iter_chunked(chunk_size):
                            downloaded_bytes += len(chunk)
                            data += chunk
                        logger.info(f"Downloaded {downloaded_bytes} bytes from {url}")
                        return data, resp.content_type
                    else:
                        logger.error(f"Failed to download source: {resp.status}")
                        return None
        except asyncio.TimeoutError as e:
            logger.exception(f"Timeout error occurred while downloading {url}.")
            if retry_count == max_retries - 1:
                logger.warning(
                    f"Reached maximum number of retries ({max_retries}). Giving up."
                )
                raise e

        except aiohttp.ClientError as e:
            logger.exception(f"Client error occurred while downloading {url}: {e}")
            if retry_count == max_retries - 1:
                logger.warning(
                    f"Reached maximum number of retries ({max_retries}). Giving up."
                )
                raise e

        except Exception as e:
            logger.exception(f"Error occurred while downloading {url}: {e}")
            if retry_count == max_retries - 1:
                logger.warning(
                    f"Reached maximum number of retries ({max_retries}). Giving up."
                )
                raise e

    return None


async def default_http_downloader(source_id=None, source_url=None, **kwargs):
    """
    Downloads data from HTTP sources

    Downloads data from a source URL using the aiohttp library. If the download fails, the function will retry up to 5 times.
    requests.

    Returns:
        None
    """
    try:
        return await simple_url_download(source_url)
    except Exception as e:
        logger.error(f"Error occurred while downloading {source_url}: {e}")
        return None, None


async def country_downloader(
    source_id=None,
    source_url=None,
    params_type=None,
    params_url=None,
    params_codes=None,
    storage_manager: AzureBlobStorageManager = None,
):
    """
    Asynchronously downloads country data from a specified source URL and adds the data to a DataFrame containing country codes and associated metadata. The metadata is obtained by downloading and processing a JSON file stored in an Azure Blob Storage container.

    Args:
        source_id (str): A unique identifier for the data source.
        source_url (str): The base URL of the source from which to download country data.
        params_type (str): The type of operation to perform on the country codes DataFrame. Default is None.
        params_url (str): The URL within the parameters
        params_codes (str): A pipe-separated string of country codes to use as parameters for the BATCH_ADD operation. Default is None.

    Returns:
        A list containing the downloaded data in CSV format and a string indicating the MIME type of the data. If an error occurs during the download, returns [None, None].

    Raises:
        None.

    Example:
        csv_data, mime_type = await country_downloader(source_id='HDR', source_url='https://example.com/countries/', params_type='BATCH_ADD', params_url='https://example.com/params.csv', params_codes='USA|GBR|FRA')
        :param params_codes:
        :param params_url:
        :param params_type:
        :param source_url:
        :param source_id:
        :param storage_manager:

    """
    try:
        # FIXME: USES A TEMPFILE advantage: can handle large files and doesn't require the file to be loaded into memory, disadvantage: slower
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_file = os.path.join(temp_dir, "country_territory_groups.json")
            storage_manager.download(
                os.path.join(
                    "DataFuturePlatform",
                    "pipeline",
                    "config",
                    "utilities",
                    "country_territory_groups.cfg",
                ),
                temp_file,
            )
            with open(temp_file, "r") as config_file:
                configparser = ConfigParser()
                configparser.read_file(config_file)
                countries_territory_data = {}
                for section in configparser.sections():
                    countries_territory_data[section] = {}
                    for key, value in configparser.items(section):
                        countries_territory_data[section][key] = value
            country_territories_data_list = []
            for key, data in countries_territory_data.items():
                data.update({"Alpha-3 code-1": key})
                country_territories_data_list.append(data)
            countries_territory_dataframe = pd.DataFrame(country_territories_data_list)

        # TODO: DOESNT USE A TEMPFILE: advantage: faster, disadvantage: requires the file to be loaded into memory
        # countries_territory_data = storage_manager.get_utility_file('country_territory_groups.cfg')
        # country_territories_data_list = []
        # for key, data in countries_territory_data.items():
        #     data.update({'Alpha-3 code-1': key})
        #     country_territories_data_list.append(data)
        # countries_territory_dataframe = pd.DataFrame(country_territories_data_list)

        countries_territory_dataframe.rename(
            columns={"Alpha-3 code-1": "Alpha-3 code"}, inplace=True
        )
        # replace empty values with NaN and convert the latitude and longitude columns to float64 type
        countries_territory_dataframe[
            "longitude (average)"
        ] = countries_territory_dataframe["longitude (average)"].replace(r"", np.NaN)
        countries_territory_dataframe[
            "latitude (average)"
        ] = countries_territory_dataframe["latitude (average)"].astype(np.float64)
        countries_territory_dataframe[
            "longitude (average)"
        ] = countries_territory_dataframe["longitude (average)"].astype(np.float64)

        # create a new dataframe with only the alpha-3 code column from the countries_territory_dataframe
        country_codes_df = pd.DataFrame(columns=["Alpha-3 code"])
        country_codes_df["Alpha-3 code"] = countries_territory_dataframe[
            "Alpha-3 code"
        ]  # only has the alpha-3 code column
        logger.info("Processed country_territory_groups.json")

        # create a list of tasks to download the country data for each country code and wait for all tasks to complete
        tasks = []
        for index, row in country_codes_df.iterrows():
            row = country_codes_df.iloc[index]
            logger.info(
                f"Downloading {row['Alpha-3 code']} from {source_url + row['Alpha-3 code'].lower()}"
            )
            text_response_task = asyncio.create_task(
                simple_url_download(
                    source_url + row["Alpha-3 code"].lower(), timeout=DEFAULT_TIMEOUT
                )
            )
            tasks.append(text_response_task)
        responses = await asyncio.gather(
            *tasks
        )  # list of responses in bytes in the format [bytes, content_type]
        for i, response in enumerate(
            responses
        ):  # response is a list of bytes and content type as follows: [bytes, content_type]
            # if the response is not None and the length of the response is greater than 0, then convert the response to a dataframe and add the data to the country_codes_df dataframe
            if response is not None and len(response[0]) > 0:
                country_indicators_df = pd.read_json(
                    io.StringIO(response[0].decode("utf-8"))
                )
                for country_id, country_row in country_indicators_df.iterrows():
                    column_name = "_".join(
                        [
                            country_row["indicator"].rsplit(" ")[0],
                            str(country_row["year"]),
                        ]
                    )
                    country_codes_df.at[i, column_name] = country_row["value"]

        # if the params_type is BATCH_ADD, then download the data from the params_url and add the data to the country_codes_df dataframe
        if params_type == "BATCH_ADD":
            text_response = await simple_url_download(
                params_url, timeout=DEFAULT_TIMEOUT
            )
            region_dataframe = pd.read_csv(
                io.StringIO(text_response[0].decode("utf-8"))
            )
            selected_dataframe = region_dataframe[
                region_dataframe["iso3"].isin(params_codes.split("|"))
            ]
            selected_dataframe["iso3"] = selected_dataframe["country"]
            selected_dataframe.rename(columns={"iso3": "Alpha-3 code"}, inplace=True)
            selected_dataframe = selected_dataframe[country_codes_df.columns]
            country_codes_df = pd.concat(
                [country_codes_df, selected_dataframe], ignore_index=True
            )

            # create a csv file from the country_codes_df dataframe and return the csv file as bytes
            csv_data = country_codes_df.to_csv(index=False).encode("utf-8")
            logger.info(f"Successfully downloaded {source_id}")
            return csv_data, "text/csv"
    except Exception as e:
        raise e


async def cpia_downloader(
    source_id=None,
    source_url=None,
    params_type=None,
    params_url=None,
    params_codes=None,
):
    pass


async def get_downloader(
    source_id: str, source_url: str, storage_manager: "AzureBlobStorageManager"
):
    url = ""
    saveAs = ""
    basePath = ""
    userData = ""
    source_downloaded_path = ""
    logging.info(f"Downloading {source_id + saveAs} from {source_url}")

    try:
        await storage_manager.delete_blob(basePath + saveAs)
        logging.info(f"Deleted existing file at {basePath + saveAs}")
    except Exception as e:
        logging.error("File delete failed")

    file_parts = saveAs.split("/")
    saveAs = file_parts[-1]

    key, value = userData.split("=")
    parameters = ast.literal_eval(value)
    logging.info(
        f"URL: {url}, Save As: {saveAs}, Base Path: {basePath}, Parameters: {parameters}"
    )

    async with aiohttp.ClientSession() as session:
        async with session.get(url, **{key: parameters}, timeout=900) as response:
            response_content = await response.read()

    await storage_manager.upload_blob(
        data=response_content, dst_path=source_downloaded_path, overwrite=True
    )
    logging.info(f"File saved to {basePath + source_downloaded_path + saveAs}")


async def post_downloader(
    source_id: str, source_url: str, storage_manager: "AzureBlobStorageManager"
):
    url = ""
    saveAs = ""
    basePath = ""
    userData = ""
    source_downloaded_path = ""
    logging.info(f"Downloading {source_id + saveAs} from {source_url}")

    try:
        await storage_manager.delete_blob(basePath + saveAs)
        logging.info(f"Deleted existing file at {basePath + saveAs}")
    except Exception as e:
        logging.error("File delete failed")

    file_parts = saveAs.split("/")
    saveAs = file_parts[-1]

    parameters = ast.literal_eval(userData.rsplit("=")[1])
    if userData.rsplit("=")[0] == "headers":
        logging.info("Header")
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=parameters, timeout=900) as response:
                response_content = await response.read()
    elif userData.rsplit("=")[0] == "params":
        logging.info("Parameters")
        async with aiohttp.ClientSession() as session:
            async with session.post(url, params=parameters, timeout=900) as response:
                response_content = await response.read()
    else:
        logging.info("Data")
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=parameters, timeout=900) as response:
                response_content = await response.read()

    await storage_manager.upload_blob(
        data=response_content, dst_path=source_downloaded_path, overwrite=True
    )
    logging.info(f"File saved to {basePath + source_downloaded_path + saveAs}")


async def get_nested_zip_downloader(
    source_id: str, source_url: str, storage_manager: "AzureBlobStorageManager"
):
    url = ""
    saveAs = ""
    basePath = ""
    userData = ""
    source_downloaded_path = ""
    logging.info(f"Downloading {source_id + saveAs} from {source_url}")

    try:
        await storage_manager.delete_blob(basePath + saveAs)
        logging.info(f"Deleted existing file at {basePath + saveAs}")
    except Exception as e:
        logging.error("File delete failed")

    file_parts = saveAs.split("/")
    saveAs = file_parts[-1]

    print(url, saveAs, basePath)

    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=900) as response:
            response_content = await response.read()

    zip_file_name = saveAs.split(".")[0] + ".zip"
    with open(zip_file_name, "wb") as f:
        f.write(response_content)

    with zipfile.ZipFile(zip_file_name) as zip_ref:
        zip_ref.extractall(saveAs.split(".")[0])

    zip_parts = userData.replace("/", "").split(".zip")[:-1]
    accumulated_path = saveAs.split(".")[0] + "/"
    for part in zip_parts:
        with zipfile.ZipFile(accumulated_path + part + ".zip") as zip_ref:
            zip_ref.extractall(accumulated_path + part)
        accumulated_path += part + "/"

    zip_content = open(
        saveAs.split(".")[0] + "/" + userData.replace(".zip", ""), "r"
    ).readlines()

    await storage_manager.upload_blob(
        data="".join(zip_content),
        dst_path=basePath + source_downloaded_path + saveAs,
        overwrite=True,
    )

    current_path = os.getcwd()

    dir_to_del = saveAs.split(".")[0] + ".zip"
    path = os.path.join(current_path, dir_to_del)
    os.remove(path)

    dir_to_del = saveAs.split(".")[0]
    path = os.path.join(current_path, dir_to_del)
    shutil.rmtree(path)


async def call_function(function_name: str, *args, **kwargs) -> Any:
    """
    Asynchronously call a function by name, passing in any arguments specified.

    Parameters:
    - function_name (str): the name of the function to call
    - *args: any arguments to pass to the function

    Returns:
    - the result of the function call, or None if the function name is None

    Example usage:
    ```
    async def my_function(arg1, arg2):
        # do some work
        return result

    result = await call_function('my_function', arg1, arg2)
    ```
    """
    if function_name is not None:
        return await globals()[function_name](*args, **kwargs)
    else:
        return None


async def retrieval() -> None:
    """
    Asynchronously retrieves data from multiple sources using Azure Blob Storage, and uploads the results to a new Blob.

    This function first lists all the blobs in the specified container that have a certain prefix, and then asynchronously
    retrieves data from each blob by calling a downloader function specified in the blob's metadata. The downloaded data
    is then uploaded to a new blob using a separate upload function.

    Returns:
    - None

    Example usage:
    ```
    await retrieval()
    ```
    """
    storage_manager = AzureBlobStorageManager.create_instance(
        connection_string=CONNECTION_STRING,
        container_name=CONTAINER_NAME,
    )

    sources = storage_manager.get_source_config()
    tasks = []

    async def upload_source_file(
        bytes_data: bytes = None, cont_type: str = None, save_as: str = None
    ):
        temp_dir = tempfile.TemporaryDirectory()
        temp_file_path = os.path.join(temp_dir.name, f"{save_as}")
        with open(temp_file_path, "wb") as temp_file:
            temp_file.write(bytes_data)
            storage_manager.upload(
                dst_path=os.path.join(ROOT_FOLDER, "sources", "raw", save_as),
                src_path=temp_file_path,
                content_type=cont_type,
                overwrite=True,
            )
        temp_dir.cleanup()

    for (
        source_id,
        source_config,
    ) in sources.items():  # sid = source id, config = source config
        logger.info(f"Downloading {source_id} from {source_config['url']}.")
        if "downloader_params" in source_config:
            params = source_config["downloader_params"]
            data, content_type = await call_function(
                source_config["downloader_function"],
                source_id=source_id,
                source_url=source_config["url"],
                params_type=params["type"],
                params_url=params["url"],
                params_codes=params["codes"],
                storage_manager=storage_manager,
            )
        else:
            data, content_type = await default_http_downloader(
                source_id=source_id, source_url=source_config["url"]
            )
        logger.info(f"Downloaded {source_id} from {source_config['url']}.")
        logger.info(f"Uploading {source_id} to blob storage.")

        upload_task = asyncio.create_task(
            upload_source_file(
                bytes_data=data,
                cont_type=content_type,
                save_as=source_config["save_as"],
            )
        )
        logger.info(f"Uploaded {source_config['save_as']} to blob storage.")
        tasks.append(upload_task)
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    import asyncio

    logging.basicConfig()
    logger = logging.getLogger("azure.storage.blob")
    logging_stream_handler = logging.StreamHandler()
    logging_stream_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s-%(filename)s:%(funcName)s:%(lineno)d:%(levelname)s:%(message)s",
            "%Y-%m-%d %H:%M:%S",
        )
    )
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    logger.addHandler(logging_stream_handler)
    logger.name = __name__
    asyncio.run(retrieval())
