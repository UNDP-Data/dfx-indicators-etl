import base64
import json
import logging

import aiohttp

from .http import simple_url_post

__all__ = [
    "sipri_downloader",
]

DEFAULT_TIMEOUT = aiohttp.ClientTimeout(
    total=120, connect=20, sock_connect=20, sock_read=20
)

logger = logging.getLogger(__name__)


async def sipri_downloader(**kwargs) -> tuple[bytes, str]:
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
        formatted_parameters = {
            "params": json.dumps(
                {"type": "json", "value": parameters},
                separators=(",", ":"),
                default=lambda x: x.__dict__,
            )
        }
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
