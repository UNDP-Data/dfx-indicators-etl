import ast
import asyncio
import io
import logging
import zipfile
from typing import Literal

import aiohttp
import tqdm
from aiohttp import ClientTimeout

__all__ = [
    "simple_url_get",
    "simple_url_post",
    "default_http_downloader",
    "get_downloader",
    "post_downloader",
    "zip_content_downloader",
]

DEFAULT_TIMEOUT = aiohttp.ClientTimeout(
    total=120, connect=20, sock_connect=20, sock_read=20
)

logger = logging.getLogger(__name__)


async def make_request(
    url: str,
    method: str = Literal["GET", "POST"],
    timeout: ClientTimeout = DEFAULT_TIMEOUT,
    max_retries: int = 5,
    **kwargs,
) -> tuple[bytes, str] | tuple[None, None] | None:
    if kwargs.get("type") == "json":
        request_args = {"json": kwargs.get("value")}
    elif kwargs.get("type") == "params":
        request_args = {"params": kwargs.get("value")}
    elif kwargs.get("type") == "data":
        request_args = {"data": kwargs.get("value")}
    else:
        request_args = {"headers": kwargs.get("value")}

    async with aiohttp.ClientSession() as session:
        for retry_count in range(max_retries):
            try:
                if method == "GET":
                    func = session.get
                elif method == "POST":
                    func = session.post
                else:
                    raise ValueError("Method must be either 'GET' or 'POST'")
                async with func(
                    url, timeout=timeout, **request_args, verify_ssl=False
                ) as resp:
                    if resp.status == 200:
                        with tqdm.tqdm(total=resp.content.total_bytes) as pbar:
                            downloaded_bytes = 0
                            chunk_size = 1024 * 200

                            data = b""
                            logger.info(f"Downloading {url}")
                            async for chunk in resp.content.iter_chunked(chunk_size):
                                downloaded_bytes += len(chunk)
                                data += chunk
                                pbar.update(len(chunk))
                            logger.debug(
                                f"Downloaded {downloaded_bytes} bytes from {url}"
                            )
                            return data, resp.content_type
                    else:
                        logger.error(f"Failed to download source: {resp.status}")
                        raise Exception(
                            f"Failed to download data from {url}. Encountered status error {resp.status}"
                        )

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


async def simple_url_get(
    url: str, **kwargs
) -> tuple[bytes, str] | tuple[None, None] | None:
    """
    Downloads the content in bytes from a given URL using the aiohttp library.

    :param url: The URL to download.
    :param kwargs: Optional dictionary of URL parameters to include in the request.
    :return: a tuple containing the downloaded content and the content type, or None if the download fails.
    """
    try:
        result = await make_request(url=url, method="GET", **kwargs)
        return result
    except Exception as e:
        raise e


async def simple_url_post(
    url: str, **kwargs
) -> tuple[bytes, str] | tuple[None, None] | None:
    """
    Sends a POST request to a given URL using the aiohttp library and returns the content in bytes.

    :param url: The URL to send the POST request to.
    :param kwargs: Additional arguments to pass to the session.post method (headers, params, data).
    :return: a tuple containing the downloaded content and the content type, or None if the request fails.
    """
    assert kwargs.get("type") is not None and kwargs.get("value") is not None
    try:
        result = await make_request(url=url, method="POST", **kwargs)
        return result
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
        return await simple_url_get(source_url)
    except Exception as e:
        logger.error(f"Error occurred while downloading {source_url}: {e}")
        raise e


async def get_downloader(**kwargs) -> tuple[bytes, str]:
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
        response_content, _ = await simple_url_get(source_url, **request_params)

        logging.info(f"Successfully downloaded {source_id} from {source_url}")

        return response_content, "text/csv"
    except Exception as e:
        raise e


async def post_downloader(**kwargs) -> tuple[bytes, str]:
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
    try:
        request_params = ast.literal_eval(kwargs.get("request_params"))
        request_params["value"] = ast.literal_eval(request_params["value"])
        response_content, _ = await simple_url_post(source_url, **request_params)

        logging.info(f"Successfully downloaded {source_id} from {source_url}")

        return response_content, "text/csv"
    except Exception as e:
        raise e


async def zip_content_downloader(**kwargs) -> tuple[bytes, str]:
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

    response_content, _ = await simple_url_get(source_url)

    # print response type
    # logging.info(f"Response type: {type(response_content)}")
    # exit()
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
