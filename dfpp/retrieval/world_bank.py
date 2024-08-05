import io
import logging
import time
import zipfile
from typing import Tuple
from urllib.parse import urlencode

import pandas as pd

from .http import simple_url_get

__all__ = [
    "cpia_downloader",
    "rcc_downloader",
]


logger = logging.getLogger(__name__)


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

        data, _ = await simple_url_get(source_url)
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


async def rcc_downloader(**kwargs) -> Tuple[bytes, str]:
    """
    Downloads content from RCC, and returns it as a CSV.
    :param kwargs: keyword arguments containing the following keys:
                   source_url: The URL to download the content from.
                   source_id: The identifier of the source.
    :return: A tuple containing the raw content data and content type.
    """
    column_names = [
        "emergency",
        "country_name",
        "region",
        "iso3",
        "admin_level_1",
        "indicator_id",
        "subvariable",
        "indicator_name",
        "thematic",
        "thematic_description",
        "topic",
        "topic_description",
        "indicator_description",
        "type",
        "question",
        "indicator_value",
        "nominator",
        "error_margin",
        "denominator",
        "indicator_month",
        "category",
        "gender",
        "age_group",
        "age_info",
        "target_group",
        "indicator_matching",
        "representativeness",
        "limitation",
        "indicator_comment",
        "source_id",
        "organisation",
        "title",
        "details",
        "authors",
        "methodology",
        "sample_size",
        "target_pop",
        "scale",
        "quality_check",
        "access_type",
        "source_comment",
        "publication_channel",
        "link",
        "source_date",
        "sample_type",
    ]
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
                "indicator_id": "PRA003",
                "limit": f"{page_limit}",
                "offset": f"{offset_number}",
                "include_header": 1,
            }
            url = f"{source_url}?{urlencode(parameters)}"
            response_content, _ = await simple_url_get(url)
            response_content = response_content.decode("utf-8")
            country_df = pd.read_csv(io.StringIO(response_content))

            if country_df.empty:
                break
            else:
                df = pd.concat([df, country_df], ignore_index=True)
                if len(country_df.index) < page_limit:
                    break
                offset_number += page_limit

        # Convert dataframe to csv in bytes format
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        return csv_bytes, "text/csv"
    except Exception as e:
        raise e
