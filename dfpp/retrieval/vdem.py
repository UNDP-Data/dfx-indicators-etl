import io
import logging
import zipfile

import aiohttp

from .http import simple_url_get

__all__ = [
    "vdem_downloader",
]

DEFAULT_TIMEOUT = aiohttp.ClientTimeout(
    total=120, connect=20, sock_connect=20, sock_read=20
)

logger = logging.getLogger(__name__)


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
        zipped_bytes_data, content_type = await simple_url_get(url=url)
        with io.BytesIO(zipped_bytes_data) as zip_file:
            with zipfile.ZipFile(zip_file) as zip_f:
                with zip_f.open(file_name) as csv_file:
                    csv_data = csv_file.read()
        return csv_data, "text/csv"
    except Exception as e:
        logging.error(f"An error occurred while downloading {url}: {e}")
        raise e
