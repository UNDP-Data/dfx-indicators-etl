from __future__ import annotations
from io import StringIO
from traceback import print_exc
import ast
import base64
import io
import json
import logging
import time
import zipfile
from typing import Any, Tuple, List, Dict
from urllib.parse import urlencode
from dotenv import load_dotenv
import os
import aiohttp
import numpy as np
import pandas as pd
from aiohttp import ClientTimeout
from dfpp.storage import StorageManager
from dfpp.constants import STANDARD_KEY_COLUMN
from dfpp.utils import chunker
import asyncio

DEFAULT_TIMEOUT = aiohttp.ClientTimeout(total=120,connect=5, sock_connect=5, sock_read=5)

logger = logging.getLogger(__name__)


async def simple_url_download(
        url: str, timeout: ClientTimeout = DEFAULT_TIMEOUT, max_retries: int = 5, **kwargs
) -> tuple[bytes, str] | tuple[None, None] | None:
    """
    Downloads the content in bytes from a given URL using the aiohttp library.

    :param url: The URL to download.
    :param timeout: The maximum amount of time to wait for a response from the server, in seconds.
    :param max_retries: The maximum number of times to retry the download if an error occurs.
    :param params: Optional dictionary of URL parameters to include in the request.
    :return: a tuple containing the downloaded content and the content type, or None if the download fails.
    """
    if kwargs.get('value') is None:
        request_args = {}
    if kwargs.get('type') == 'json':
        request_args = {'json': kwargs.get('value')}
    elif kwargs.get('type') == 'params':
        request_args = {'params': kwargs.get('value')}
    elif kwargs.get('type') == 'data':
        request_args = {'data': kwargs.get('value')}
    else:
        request_args = {'headers': kwargs.get('value')}
    try:
        async with aiohttp.ClientSession() as session:
            for retry_count in range(max_retries):
                try:
                    async with session.get(url, timeout=timeout, **request_args) as resp:
                        if resp.status == 200:
                            downloaded_bytes = 0
                            chunk_size = 1024 * 200

                            data = b""
                            logger.info(f"Downloading {url}")
                            async for chunk in resp.content.iter_chunked(chunk_size):
                                downloaded_bytes += len(chunk)
                                data += chunk
                            logger.debug(f"Downloaded {downloaded_bytes} bytes from {url}")
                            return data, resp.content_type
                        else:
                            raise Exception(
                                f'Failed to download data from {url}. Encountered status error {resp.status}')

                except asyncio.TimeoutError as e:
                    logger.error(f"Timeout error occurred while downloading {url}.")
                    if retry_count == max_retries - 1:
                        logger.warning(
                            f"Reached maximum number of retries ({max_retries}). Giving up."
                        )
                        raise e

                except aiohttp.ClientError as e:
                    logger.error(f"Client error occurred while downloading {url}: {e}")
                    if retry_count == max_retries - 1:
                        logger.warning(
                            f"Reached maximum number of retries ({max_retries}). Giving up."
                        )
                        raise e

                except Exception as e:
                    logger.error(f"Error occurred while downloading {url}: {e}")
                    if retry_count == max_retries - 1:
                        logger.warning(
                            f"Reached maximum number of retries ({max_retries}). Giving up."
                        )
                        raise e
    except Exception as e:
        raise e


async def simple_url_post(
        url: str, timeout: ClientTimeout = DEFAULT_TIMEOUT, max_retries: int = 5, **kwargs
) -> tuple[bytes, str] | tuple[None, None] | None:
    """
    Sends a POST request to a given URL using the aiohttp library and returns the content in bytes.

    :param url: The URL to send the POST request to.
    :param timeout: The maximum amount of time to wait for a response from the server, in seconds.
    :param max_retries: The maximum number of times to retry the POST request if an error occurs.
    :param kwargs: Additional arguments to pass to the session.post method (headers, params, data).
    :return: a tuple containing the downloaded content and the content type, or None if the request fails.
    """

    parameters = json.loads(kwargs.get('params'))
    assert parameters.get('type') is not None and parameters.get('value') is not None
    if parameters.get('type') == 'json':
        request_args = {'json': parameters['value']}
    elif parameters.get('type') == 'params':
        request_args = {'params': parameters['value']}
    elif parameters.get('type') == 'data':
        request_args = {'data': parameters['value']}
    else:
        request_args = {'headers': parameters['value']}
    try:
        async with aiohttp.ClientSession() as session:
            for retry_count in range(max_retries):
                try:
                    async with session.post(url, timeout=timeout, **request_args, verify_ssl=False) as resp:
                        if resp.status == 200:
                            data = await resp.read()
                            return data, resp.content_type
                        else:
                            logger.error(f"Failed to download source: {resp.status}")
                            raise Exception(
                                f'Failed to download data from {url}. Encountered status error {resp.status}')
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
    except Exception as e:
        logger.exception(f"Error occurred while downloading {url}: {e}")
        raise e


async def default_http_downloader(**kwargs):
    """
    Downloads data from HTTP sources

    Downloads data from a source URL using the aiohttp library. If the download fails, the function will retry up to 5 times.
    requests.

    Returns:
        None
    """
    source_url = kwargs.get("source_url")
    try:
        return await simple_url_download(source_url)
    except Exception as e:
        logger.error(f"Error occurred while downloading {source_url}: {e}")
        raise e


async def country_downloader(**kwargs):
    """
    Asynchronously downloads country data from a specified source URL and adds the data to a DataFrame containing country codes and associated metadata. The metadata is obtained by downloading and processing a JSON file stored in an Azure Blob Storage container.

    Args:
        source_id (str): A unique identifier for the data source.
        source_url (str): The base URL of the source from which to download country data.
        params_type (str): The type of operation to perform on the country codes DataFrame. Default is None.
        params_url (str): The URL within the parameters
        params_codes (str): A pipe-separated string of country codes to use as parameters for the BATCH_ADD operation. Default is None.
        :param storage_manager: An instance of the AsyncAzureBlobStorageManager class.
    Returns:
        A list containing the downloaded data in CSV format and a string indicating the MIME type of the data. If an error occurs during the download, returns [None, None].

    Raises:
        None.

    Example:
        csv_data, mime_type = await country_downloader(source_id='HDR', source_url='https://example.com/countries/', params_type='BATCH_ADD', params_url='https://example.com/params.csv', params_codes='USA|GBR|FRA')

    """
    source_id = kwargs.get("source_id")
    source_url = kwargs.get("source_url")
    params_type = kwargs.get("params_type")
    params_url = kwargs.get("params_url")
    params_codes = kwargs.get("params_codes")
    storage_manager = kwargs.get("storage_manager")

    assert source_id is not None and source_url is not None and storage_manager is not None and params_url is not None and params_codes is not None and params_type is not None, "Check that required arguments are not none"
    try:

        countries_territory_data = await storage_manager.get_utility_file(
            "country_territory_groups.cfg"
        )
        country_territories_data_list = []
        for key, data in countries_territory_data.items():
            data.update({"Alpha-3 code-1": key})
            country_territories_data_list.append(data)
        countries_territory_dataframe = pd.DataFrame(country_territories_data_list)

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
                    os.path.join(source_url, row['Alpha-3 code'].lower()),
                    timeout=DEFAULT_TIMEOUT,
                )
            )
            tasks.append(text_response_task)

        responses = await asyncio.gather(
            *tasks, return_exceptions=True
        )  # list of responses in bytes in the format [bytes, content_type]

        for i, response in enumerate(
                responses
        ):  # response is a list of bytes and content type as follows: [bytes, content_type]
            # if the response is not None and the length of the response is greater than 0, then convert the response to a dataframe and add the data to the country_codes_df dataframe
            if isinstance(response, Exception):
                raise Exception(f"Error occurred while downloading {source_url}: {response}")

            if response is not None and len(response[0]) > 0:

                country_indicators_df = pd.read_json(
                    io.StringIO(response[0].decode("utf-8"))
                )

                for row_index, country_row in country_indicators_df.iterrows():
                    # logger.debug(f'processing {country_row["country"]}')
                    column_name = "_".join(
                        [
                            country_row["indicator"].rsplit(" ")[0],
                            str(country_row["year"]),
                        ]
                    )
                    logger.debug(f'processing {country_row["country"]}')

                    country_codes_df.at[i, column_name] = country_row["value"]
        # if the params_type is BATCH_ADD, then download the data from the params_url and add the data to the country_codes_df dataframe
        if params_type == "BATCH_ADD":
            logger.info(f"Downloading {source_id} from {params_url}")
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
            selected_dataframe.rename(columns={"iso3": STANDARD_KEY_COLUMN}, inplace=True)
            selected_dataframe = selected_dataframe[country_codes_df.columns]

            country_codes_df = pd.merge(country_codes_df, selected_dataframe, on=STANDARD_KEY_COLUMN)
            # country_codes_df = pd.concat(
            #     [country_codes_df, selected_dataframe], ignore_index=True
            # )

        # create a csv file from the country_codes_df dataframe and return the csv file as bytes
        csv_data = country_codes_df.to_csv(index=False).encode("utf-8")
        logger.info(f"Successfully downloaded {source_id}")
        return csv_data, "text/csv"

    except Exception as e:
        raise e


async def cpia_downloader(**kwargs):
    """
    Download the CPIA data from the World Bank API.
    :param source_url: The URL to download the data from.
    :return: a tuple containing the data and the mime type
    """

    exception_list = ["CPIA_RLPR.csv", "CPIA_SPCA.csv", "CW_ADAPTATION.csv"]
    source_url = kwargs.get("source_url")
    assert source_url, "source_url not provided"
    try:

        data, _ = await simple_url_download(source_url, timeout=DEFAULT_TIMEOUT)
        with io.BytesIO(data) as zip_file:
            with zipfile.ZipFile(zip_file) as zip_f:
                if kwargs.get("source_save_as") not in exception_list:
                    for file in zip_f.namelist():
                        if "Metadata" not in file:
                            csv_file_name = file
                            break
                else:
                    csv_file_name = kwargs.get("params_file")
                with zip_f.open(csv_file_name) as f:
                    csv_data = f.read()
                    logger.info(f"Successfully downloaded {kwargs.get('source_id')}")
                    return csv_data, "text/csv"
    except Exception as e:
        raise e


async def get_downloader(**kwargs) -> Tuple[bytes, str]:
    """
    Downloads content using a GET request, and returns it as a CSV.

    :param kwargs: Keyword arguments containing the following keys:
                   source_id: The identifier of the source.
                   source_url: The URL to download the content from.
                   user_data: A dictionary containing the parameters for the GET request.
    :return: A tuple containing the raw content data and content type.
    """
    source_id = kwargs.get("source_id")
    source_url = kwargs.get("source_url")
    assert source_url, "source_url not provided"
    assert source_id, "source_id not provided"
    logging.info(f"Downloading {source_id} from {source_url}")
    try:
        request_params = ast.literal_eval(kwargs.get("request_params"))
        request_params["value"] = ast.literal_eval(request_params["value"])
        response_content, _ = await simple_url_download(
            source_url,
            timeout=DEFAULT_TIMEOUT,
            max_retries=5,
            **request_params,
        )

        logging.info(f"Successfully downloaded {source_id} from {source_url}")

        return response_content, "text/csv"
    except Exception as e:
        raise e


async def post_downloader(**kwargs) -> Tuple[bytes, str]:
    """
    Downloads content using a POST request, and returns it as a CSV.

    :param kwargs: Keyword arguments containing the following keys:
                   source_id: The identifier of the source.
                   source_url: The URL to send the POST request to.
                   user_data: A dictionary containing either headers, params, or data for the POST request.
    :return: A tuple containing the raw content data and content type.
    """
    source_id = kwargs.get("source_id")
    source_url = kwargs.get("source_url")
    assert source_url, "source_url not provided"
    assert source_id, "source_id not provided"
    logging.info(f"Downloading {source_id} from {source_url}")
    print(kwargs.get("request_params"))
    try:
        response_content, _ = await simple_url_post(
            source_url,
            timeout=DEFAULT_TIMEOUT,
            max_retries=5,
            params=kwargs.get("request_params"),
        )

        logging.info(f"Successfully downloaded {source_id} from {source_url}")

        return response_content, "text/csv"
    except Exception as e:
        raise e


async def zip_content_downloader(**kwargs) -> Tuple[bytes, str]:
    """
    Downloads a ZIP, or nested ZIP file, using a GET request, extracts its content, and returns it as a CSV.

    :param kwargs: Keyword arguments containing the following keys:
                   source_id: The identifier of the source.
                   source_url: The URL to download the content from.
                   params_file: A string containing the file path to locate the required content within the nested ZIP file.
    :return: A tuple containing the raw content data and content type.
    """
    source_id = kwargs.get("source_id")
    source_url = kwargs.get("source_url")
    params_file = kwargs.get("params_file")
    logging.info(f"Downloading {source_id} from {source_url}")
    assert source_url, "source_url not provided"
    assert source_id, "source_id not provided"
    assert params_file, "params_file not provided"

    response_content, _ = await simple_url_download(
        source_url, timeout=DEFAULT_TIMEOUT, max_retries=5
    )

    try:
        with zipfile.ZipFile(io.BytesIO(response_content), "r") as zip_file:

            if ".zip" in params_file:
                parts = params_file.split("/")
                outer_zip_name, inner_path = parts[0], "/".join(parts[1:])

                with zip_file.open(outer_zip_name) as outer_zip_file:
                    with zipfile.ZipFile(
                            io.BytesIO(outer_zip_file.read()), "r"
                    ) as inner_zip:
                        target_file = inner_zip.open(inner_path)
            else:
                target_file = zip_file.open(params_file)

            if target_file:
                csv_content = target_file.read()

                logging.info(f"Successfully downloaded {source_id} from {source_url}")

                return csv_content, "text/csv"
    except zipfile.BadZipFile as e:
        logging.error(f"BadZipFile error: {e}")
        raise
    except Exception as e:
        logging.error(f"An error occurred while processing the ZIP file: {e}")
        raise e


async def rcc_downloader(**kwargs) -> Tuple[bytes, str]:
    """
    Downloads content from RCC, and returns it as a CSV.
    :param kwargs: keyword arguments containing the following keys:
                   source_url: The URL to download the content from.
                   source_id: The identifier of the source.
    :return: A tuple containing the raw content data and content type.
    """
    column_names = ['emergency', 'country_name', 'region', 'iso3', 'admin_level_1',
                    'indicator_id', 'subvariable', 'indicator_name', 'thematic',
                    'thematic_description', 'topic', 'topic_description',
                    'indicator_description', 'type', 'question', 'indicator_value',
                    'nominator', 'error_margin', 'denominator', 'indicator_month',
                    'category', 'gender', 'age_group', 'age_info', 'target_group',
                    'indicator_matching', 'representativeness', 'limitation',
                    'indicator_comment', 'source_id', 'organisation', 'title', 'details',
                    'authors', 'methodology', 'sample_size', 'target_pop', 'scale',
                    'quality_check', 'access_type', 'source_comment', 'publication_channel',
                    'link', 'source_date', 'sample_type']
    refresh_time = 1
    page_limit = 50
    offset_number = 0
    source_url = kwargs.get("source_url")
    assert source_url, "source_url not provided"
    df = pd.DataFrame(columns=column_names)
    try:
        while True:
            time.sleep(refresh_time)
            parameters = {
                'indicator_id': 'PRA003',
                'limit': f'{page_limit}',
                'offset': f'{offset_number}',
                'include_header': 1
            }
            url = f'{source_url}?{urlencode(parameters)}'
            response_content, _ = await simple_url_download(
                url, timeout=DEFAULT_TIMEOUT, max_retries=5
            )
            response_content = response_content.decode('utf-8')
            country_df = pd.read_csv(io.StringIO(response_content))

            if country_df.empty:
                break
            else:
                df = pd.concat([df, country_df], ignore_index=True)
                if len(country_df.index) < page_limit:
                    break
                offset_number += page_limit

        # Convert dataframe to csv in bytes format
        csv_bytes = df.to_csv(index=False).encode('utf-8')
        return csv_bytes, "text/csv"
    except Exception as e:
        raise e


async def sipri_downloader(**kwargs) -> Tuple[bytes, str]:
    """
    Downloads content using a custom POST request logic from SIPRI, and returns the raw data and content type.

    :param kwargs: Keyword arguments containing the following keys:
                   source_url: The URL to send the POST request to.
                   source_id: The identifier of the source.
    :return: A tuple containing the raw content data and content type.
    """
    source_url = kwargs.get("source_url")
    source_id = kwargs.get("source_id")
    assert source_url, "source_url not provided"
    assert source_id, "source_id not provided"
    logging.info(f"Downloading {source_id} from {source_url}")

    # Set up parameters
    parameters = {
        "regionalTotals": False,
        "currencyFY": False,
        "currencyCY": False,
        "constantUSD": False,
        "currentUSD": False,
        "shareOfGDP": False,
        "perCapita": False,
        "shareGovt": True,
        "regionDataDetails": False,
        "getLiveData": False,
        "yearFrom": None,
        "yearTo": None,
        "yearList": [2016, 2021],
        "countryList": [],
    }
    try:
        formatted_parameters = {'params': json.dumps({'type': 'json', 'value': parameters}, separators=(',', ':'),
                                                     default=lambda x: x.__dict__)}
        # Execute the POST request
        response_content, _ = await simple_url_post(
            source_url, timeout=DEFAULT_TIMEOUT, max_retries=5, **formatted_parameters
        )

        # Process the response
        data = json.loads(response_content)
        file_bytes = bytes(data["Value"], "utf8")
        csv_data = base64.b64decode(file_bytes)

        return csv_data, "text/csv"
    except Exception as e:
        logging.error(f"An error occurred while downloading {source_id}: {e}")
        raise e


async def vdem_downloader(**kwargs):
    """
    :param url: source url to download from
    :return:
    """
    url = kwargs.get("source_url")
    file_name = kwargs.get("params_file")
    assert url, "source_url not provided"
    assert file_name, "params_file not provided"
    try:
        zipped_bytes_data, content_type = await simple_url_download(url=url)
        with io.BytesIO(zipped_bytes_data) as zip_file:
            with zipfile.ZipFile(zip_file) as zip_f:
                with zip_f.open(file_name) as csv_file:
                    csv_data = csv_file.read()
        return csv_data, "text/csv"
    except Exception as e:
        logging.error(f"An error occurred while downloading {url}: {e}")
        raise e


async def call_function(function_name, *args, **kwargs) -> Any:
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
    function = globals().get(function_name)
    assert function is not None, f"Function {function_name} is not defined"
    if function is not None:
        return await function(*args, **kwargs)
    else:
        raise ValueError(f"Function {function} is not defined or not callable")


async def download_for_indicator(indicator_cfg: Dict[str, Any], source_cfg: Dict[str, Any],
                                 storage_manager: StorageManager):
    """

    :param indicator_cfg:
    :param source_cfg:
    :param storage_manager:
    :return: number of downloaded/uploaded bytes
    """
    source_id = indicator_cfg['indicator']['source_id']
    assert source_id is not None, "Source ID must be specified in indicator config"
    # try:
    logger.info(
        f"Starting to download source {source_id} from {source_cfg['source']['url']} using {source_cfg['source']['downloader_function']}.")

    downloader_params = source_cfg['downloader_params']
    # requests_cache.install_cache("cache_name",
    #                              expire_after=3600)  # Cache data for one hour (in seconds)
    if downloader_params.get('request_params') is None:
        data, content_type = await call_function(
            source_cfg['source']["downloader_function"],
            source_id=source_id,
            source_url=source_cfg['source'].get("url"),
            source_save_as=source_cfg['source'].get("save_as"),
            storage_manager=storage_manager,
            params_file=downloader_params.get('file'),
        )
    else:
        request_params = json.loads(downloader_params.get('request_params'))
        params_file = downloader_params.get('file')
        params_type = request_params.get("type"),
        params_url = json.loads(request_params.get("value").replace("'", '"')).get("url"),
        params_codes = downloader_params.get("codes"),
        data, content_type = await call_function(
            source_cfg['source']["downloader_function"],
            source_id=source_id,
            source_url=source_cfg['source'].get("url"),
            source_save_as=source_cfg['source'].get("save_as"),
            params_type=params_type,
            params_url=params_url,
            params_codes=params_codes,
            params_file=params_file,
            request_params=downloader_params.get("request_params"),
            storage_manager=storage_manager,
        )
    logger.info(f"Downloaded {source_id} from {source_cfg['source']['url']}.")
    # it makes sense to combine the download and upload here because  an indicatpr has been downloaded
    # if the source data have been downloaded and the result uploaded to azure
    if data is not None:

        dst_path = os.path.join(storage_manager.SOURCES_PATH, source_cfg['source']['save_as'])
        await asyncio.create_task(
            storage_manager.upload(
                data=data,
                content_type=content_type,
                dst_path=dst_path,
                overwrite=True
            )
        )

        return len(data)
    else:
        return 0


async def download_sources(
        indicator_ids: List | str = None,
        indicator_id_contain_filter: str = None,
        concurrent_chunk_size: int = 50
) -> list[str]:
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
    try:
        async with StorageManager() as storage_manager:
            if indicator_ids is not None and len(indicator_ids) > 0:
                indicator_cfgs = await storage_manager.get_indicators_cfg(indicator_ids=indicator_ids)
            elif indicator_id_contain_filter is not None:
                indicator_cfgs = await storage_manager.get_indicators_cfg(contain_filter=indicator_id_contain_filter)
            else:
                indicator_cfgs = await storage_manager.get_indicators_cfg()
            successful_indicator_ids = []
            unique_source_ids = set([indicator_cfg['indicator']['source_id'] for indicator_cfg in indicator_cfgs])
            download_tasks = []
            for source_id in unique_source_ids:
                indicator_cfg = list(filter(lambda x: x['indicator']['source_id'] == source_id, indicator_cfgs))[0]
                # for indicator_cfg in indicator_cfgs:
                source_id = indicator_cfg['indicator'].get('source_id')
                source_cfg = await storage_manager.get_source_cfg(source_id=source_id)
                # if source_id != "ISABO":
                #     continue
                if not await storage_manager.check_blob_exists(
                        blob_name=os.path.join(storage_manager.SOURCES_CFG_PATH, source_id.lower(),
                                               f'{source_id.lower()}.cfg')):
                    logger.warning(
                        f"Source {source_id} referenced by indicator {indicator_cfg['indicator']['indicator_id']} does not exist in the storage. So it will be skipped.")
                    continue
                if source_cfg['source']['source_type'] == "Manual":
                    logger.info(f"Skipping manual source {source_id}")
                    continue
                if source_cfg['source'].get('save_as') is None:
                    source_cfg['source']['save_as'] = f"{source_id}.{source_cfg['url'].split('.')[-1]}"
                download_task = asyncio.create_task(
                    download_for_indicator(indicator_cfg=indicator_cfg, source_cfg=source_cfg,
                                           storage_manager=storage_manager))
                download_tasks.append(download_task)
            download_results = await asyncio.gather(*download_tasks, return_exceptions=True)
            failed_downloads = []
            upload_tasks = []
            for result in download_results:
                data, content_type, error = result
                if error is not None:
                    failed_downloads.append([data, content_type, error])
                    continue

                if data is not None:
                    upload_task = asyncio.create_task(
                        storage_manager.upload(data=data, content_type=content_type,
                                               dst_path=os.path.join(storage_manager.SOURCES_PATH,
                                                                     source_cfg['source']['save_as']),
                                               overwrite=True))
                    upload_tasks.append(upload_task)
            await asyncio.gather(*upload_tasks, return_exceptions=True)
            if len(failed_downloads) > 0:
                # log failed download to file and raise exception

                raise Exception(f"Failed to download {len(failed_downloads)} sources. Please check the logs.")
            else:
                logger.info(f"Successfully downloaded {len(download_results)} sources out of {len(indicator_cfgs)}.")
    except Exception as e:
        raise e
    return successful_indicator_ids


async def download_indicator_sources(
        indicator_ids: List | str = None,
        indicator_id_contain_filter: str = None,
        concurrent_chunk_size: int = 50
) -> list[str]:


    failed_source_ids = []
    source_indicator_map = {}
    source_indicator_map1 = {}


    async with StorageManager() as storage_manager:
        logger.debug(f'Connected to Azure blob')

        indicator_configs = await storage_manager.get_indicators_cfg(indicator_ids=indicator_ids,
                                                                     contain_filter=indicator_id_contain_filter)

        sources = [indicator_cfg['indicator']['source_id'] for indicator_cfg in indicator_configs]
        unique_source_ids = set(sources)
        for c in indicator_configs:
            src = c['indicator']['source_id']
            ind = c['indicator']['indicator_id']
            if not src in source_indicator_map1:
                source_indicator_map1[src] = [ind]
            else:
                source_indicator_map1[src].append(ind)

        logger.info(
            f' {len(unique_source_ids)} sources defining {len(indicator_configs)} indicators have been detected in the config folder {storage_manager.INDICATORS_CFG_PATH}')

        for chunk in chunker(unique_source_ids, size=concurrent_chunk_size):
            download_tasks = list()
            for source_id in chunk:

                indicator_cfg = list(filter(lambda x: x['indicator']['source_id'] == source_id, indicator_configs))[0]

                source_id = indicator_cfg['indicator'].get('source_id')
                # get source config is checking for existence as well
                try:
                    source_cfg = await storage_manager.get_source_cfg(source_id=source_id)
                    if source_cfg['source']['source_type'] == "Manual":
                        logger.info(f"Skipping manual source {source_id}")
                        continue
                    if source_cfg['source'].get('save_as') is None:  # compute missing
                        save_as = f"{source_id}.{source_cfg['url'].split('.')[-1]}"
                        logger.warning(f'Source data for indicator {indicator_id} wil be saved  to  {save_as}')
                        source_cfg['source']['save_as'] = save_as
                    download_task = asyncio.create_task(
                        download_for_indicator(indicator_cfg=indicator_cfg, source_cfg=source_cfg,
                                               storage_manager=storage_manager), name=source_id)

                    download_tasks.append(download_task)
                except Exception as e:
                    logger.error(f'Failed to download/upload source {source_id} ')
                    logger.error(e)
                    continue

            logger.info(f'Downloading {len(chunk)} indicator sources concurrently')
            done, pending = await asyncio.wait(download_tasks,
                                               return_when=asyncio.ALL_COMPLETED,
                                               timeout=concurrent_chunk_size * DEFAULT_TIMEOUT.total + 10
                                               # to make 100% sure the download
                                               # never gets stuck
                                               )
            if done:
                logger.info(f'Collecting results for {len(chunk)} sources')
                for done_task in done:

                    try:
                        source_id = done_task.get_name()
                        data_size_bytes = await done_task
                        if data_size_bytes < 100:  # TODO: establish  a realistic value
                            logger.warning(f'No data was downloaded for indicator {indicator_id}')
                            failed_source_ids.append(source_id)
                        else:
                            source_indicator_map[source_id] = source_indicator_map1[source_id]
                    except Exception as e:
                        failed_source_ids.append(source_id)
                        with StringIO() as m:
                            print_exc(file=m)
                            em = m.getvalue()
                            logger.error(f'Error {em} was encountered while processing  {source_id}')

            if pending:
                logger.debug(f'{len(pending)} out of {len(chunk)} sources  have timed out')

                for pending_task in pending:

                    try:
                        source_id, indicator_id = done_task.get_name().split('::')
                        pending_task.cancel()
                        await pending_task
                        failed_source_ids.append(source_id)
                    except asyncio.CancelledError:
                        logger.debug(
                            f'Pending future for source {source_id} has been cancelled')
                    except Exception as e:

                        raise e
            download_tasks = []

    downloaded_indicators = sorted([item for sublist in source_indicator_map.values() for item in sublist])

    logger.info('#' * 200)
    logger.info(f'TASKED: {len(unique_source_ids)} sources defining {len(indicator_configs)} indicators')
    logger.info(
        f'DOWNLOADED:  {len(source_indicator_map.keys())} sources defining {len(downloaded_indicators)} indicators')
    if failed_source_ids:
        failed_indicators = []
        for fsource in failed_source_ids:
            failed_indicators += source_indicator_map1[fsource]
        logger.info(f'FAILED {len(failed_source_ids)} defining {len(failed_indicators)} indicators')
    logger.info('#' * 200)

    return downloaded_indicators


if __name__ == "__main__":
    import asyncio
    import logging

    load_dotenv(dotenv_path='./.env')

    logging.basicConfig()
    logger = logging.getLogger("azure.storage.blob")
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
    logger.name = __name__
    asyncio.run(download_indicator_sources())
