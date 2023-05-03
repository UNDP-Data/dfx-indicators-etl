import os
import io
import json
import logging
from typing import Any, Optional, Tuple
import aiohttp
import numpy as np
import pandas as pd
from azure.storage.blob.aio import BlobServiceClient, BlobClient
from azure.storage.blob import ContentSettings
import azblob
from tqdm import tqdm

CONNECTION_STRING = os.environ['AZURE_STORAGE_CONNECTION_STRING']
CONTAINER_NAME = os.environ['CONTAINER_NAME']
ROOT_FOLDER = os.environ['ROOT_FOLDER']
DEFAULT_TIMEOUT = aiohttp.ClientTimeout(total=600)


async def simple_url_download(url, timeout=DEFAULT_TIMEOUT, max_retries=5) -> Optional[Tuple[bytes, str]]:
    """
    Downloads the content in bytes from a given URL using the aiohttp library.

    :param url: The URL to download.
    :param timeout: The maximum amount of time to wait for a response from the server, in seconds.
    :param max_retries: The maximum number of times to retry the download if an error occurs.
    :return: a tuple containing the downloaded content and the content type, or None if the download fails.
    """
    async with aiohttp.ClientSession() as session:
        retry_count = 0
        while retry_count < max_retries:
            try:
                async with session.get(url, timeout=timeout) as resp:
                    if resp.status == 200:
                        content_length = int(resp.headers.get('Content-Length', 0))
                        print(f'Content length: {content_length}')
                        downloaded_bytes = 0
                        chunk_size = 1024
                        data = b''
                        progress_bar = tqdm(total=content_length, unit='B', unit_scale=True,
                                            desc=url.split('/')[-1])
                        async for chunk in resp.content.iter_chunked(chunk_size):
                            downloaded_bytes += len(chunk)
                            progress_bar.update(len(chunk))
                            data += chunk
                        progress_bar.close()
                        # data = await resp.read()  # bytes in memory
                        # print('Downloaded {} bytes'.format(len(data)))
                        return data, resp.content_type
                    else:
                        print(f'Failed to download source: {resp.status}')
                        return None
            except asyncio.TimeoutError:
                retry_count += 1
                logger.info(f'Timeout error occurred while downloading {url}.')
            except aiohttp.ClientError as e:
                retry_count += 1
                logger.info(f'Client error occurred while downloading {url}: {e}')
            except Exception as e:
                retry_count += 1
                logger.info(f'Error occurred while downloading {url}: {e}')
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
        logger.info(f'Error occurred while downloading {source_url}: {e}')
        return None, None


async def country_downloader(source_id=None, source_url=None, params_type=None, params_url=None,
                             params_codes=None):
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

    """
    try:
        async with BlobClient.from_connection_string(
                conn_str=CONNECTION_STRING,
                container_name=CONTAINER_NAME,
                blob_name=os.path.join(ROOT_FOLDER, 'Utilities', 'country_territory_groups.json')) as blob_client:
            logger.info("Downloading country_territory_groups.json")
            blob_data = await blob_client.download_blob()
            content = await blob_data.content_as_bytes()
            logger.info("Downloaded country_territory_groups.json")
            logger.info("Processing country_territory_groups.json")
            countries_territory_dataframe = pd.DataFrame(json.loads(content.decode()))
            countries_territory_dataframe.rename(columns={'Alpha-3 code-1': 'Alpha-3 code'}, inplace=True)
            countries_territory_dataframe['Longitude (average)'] = countries_territory_dataframe[
                'Longitude (average)'].replace(r'', np.NaN)
            countries_territory_dataframe['Latitude (average)'] = countries_territory_dataframe[
                "Latitude (average)"].astype(np.float64)
            countries_territory_dataframe['Longitude (average)'] = countries_territory_dataframe[
                "Longitude (average)"].astype(np.float64)
            country_codes_df = pd.DataFrame(columns=['Alpha-3 code'])
            country_codes_df['Alpha-3 code'] = countries_territory_dataframe[
                'Alpha-3 code']  # only has the alpha-3 code column
            logger.info("Processed country_territory_groups.json")
            tasks = []
            for index, row in country_codes_df.iterrows():
                row = country_codes_df.iloc[index]
                logger.debug(f"Downloading {row['Alpha-3 code']} from {source_url + row['Alpha-3 code'].lower()}")
                text_response_task = asyncio.create_task(
                    simple_url_download(source_url + row['Alpha-3 code'].lower(), timeout=DEFAULT_TIMEOUT))
                tasks.append(text_response_task)
            responses = await asyncio.gather(*tasks)  # list of responses in bytes in the format [bytes, content_type]
            for i, response in enumerate(responses): # response is a list of bytes and content type as follows: [bytes, content_type]
                if response is not None and len(response[0]) > 0:
                    country_indicators_df = pd.read_json(io.StringIO(response[0].decode('utf-8')))
                    for country_id, country_row in country_indicators_df.iterrows():
                        column_name = "_".join([country_row["indicator"].rsplit(" ")[0], str(country_row["year"])])
                        country_codes_df.at[i, column_name] = country_row["value"]
            print(country_codes_df)
            if params_type == 'BATCH_ADD':
                text_response = await simple_url_download(params_url, timeout=DEFAULT_TIMEOUT)
                region_dataframe = pd.read_csv(io.StringIO(text_response[0].decode('utf-8')))
                selected_dataframe = region_dataframe[
                    region_dataframe['iso3'].isin(params_codes.split('|'))]
                selected_dataframe['iso3'] = selected_dataframe['country']
                selected_dataframe.rename(columns={'iso3': 'Alpha-3 code'}, inplace=True)
                selected_dataframe = selected_dataframe[country_codes_df.columns]
                country_codes_df = pd.concat([country_codes_df, selected_dataframe], ignore_index=True)
                csv_data = country_codes_df.to_csv(index=False).encode('utf-8')
                print(f"Successfully downloaded {source_id}")
                return csv_data, 'text/csv'
    except Exception as e:
        raise e


async def cpia_downloader():
    pass


async def simple_upload_to_blob(blob_name: str = None, data: bytes = None, content_type: str = None) -> None:
    """
    Asynchronously uploads data to Azure Blob Storage.

    This function creates a new blob in the specified container and uploads the provided data to it. If a blob with the
    same name already exists, it will be overwritten. The content type of the uploaded data can also be specified.

    Parameters:
    - blob_name (str): the name of the new blob to create (e.g. 'myblob.txt')
    - data (bytes): the data to upload to the blob
    - content_type (str): the content type of the uploaded data (e.g. 'text/plain')

    Returns:
    - None

    Example usage:
    ```
    await simple_upload_to_blob('myblob.txt', b'Hello, world!', 'text/plain')
    ```
    """
    async with BlobServiceClient.from_connection_string(CONNECTION_STRING) as blob_service_client:
        if data is not None:
            logger.info(f"Uploading {blob_name} to blob storage.")
            await blob_service_client.get_container_client(CONTAINER_NAME).upload_blob(
                blob_name, data, overwrite=True, max_concurrency=8,
                content_settings=ContentSettings(content_type=content_type)
            )
            logger.info(f"Uploaded {blob_name} to blob storage.")
        else:
            logger.warning(f"Failed to upload {blob_name} to blob storage because data is None.")


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
    sources = await azblob.list_blobs(
        connection_string=CONNECTION_STRING,
        container_name=CONTAINER_NAME,
        prefix=os.path.join(ROOT_FOLDER, 'pipeline', 'config', 'sources')
    )

    tasks = []
    for sid, config in sources.items():  # sid = source id, config = source config
        print('249 SOURCE ID: ', sid)
        logger.info(f"Downloading {sid} from {config['url']}.")
        if 'downloader_params' in config:
            params = config['downloader_params']
            data, content_type = await call_function(config['downloader_function'],
                                                     source_id=sid,
                                                     source_url=config['url'], params_type=params['type'],
                                                     params_url=params['url'], params_codes=params['codes'])
        else:
            # [data, content_type] = await call_function(config['downloader_function'], source_id=sid, source_url=config['url'])
            data, content_type = await default_http_downloader(source_id=sid, source_url=config['url'])
        logger.info(f"Downloaded {sid} from {config['url']}.")
        logger.info(f"Uploading {sid} to blob storage.")
        upload_task = asyncio.create_task(
            simple_upload_to_blob(os.path.join(ROOT_FOLDER, 'pipeline', 'data_sources', config['save_as']), data,
                                  content_type))
        logger.info(f"Uploaded {sid} to blob storage.")
        tasks.append(upload_task)
    await asyncio.gather(*tasks)


if __name__ == '__main__':
    import asyncio

    logging.basicConfig()
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    asyncio.run(retrieval())
