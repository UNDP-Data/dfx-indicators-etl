import asyncio
import io
import logging
import os

import numpy as np
import pandas as pd

from ..constants import STANDARD_KEY_COLUMN
from .http import simple_url_get

__all__ = [
    "country_downloader",
]

logger = logging.getLogger(__name__)


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

    assert (
        source_id is not None
        and source_url is not None
        and storage_manager is not None
        and params_url is not None
        and params_codes is not None
        and params_type is not None
    ), "Check that required arguments are not none"
    try:

        m49 = await storage_manager.get_utility_file("country_territory_groups.cfg")
        records = []
        for key, data in m49.items():
            data.update({"Alpha-3 code-1": key})
            records.append(data)
        df_m49 = pd.DataFrame(records)

        df_m49.rename(columns={"Alpha-3 code-1": "Alpha-3 code"}, inplace=True)
        # replace empty values with NaN and convert the latitude and longitude columns to float64 type
        df_m49["longitude (average)"] = df_m49["longitude (average)"].replace(
            r"", np.NaN
        )
        df_m49["latitude (average)"] = df_m49["latitude (average)"].astype(np.float64)
        df_m49["longitude (average)"] = df_m49["longitude (average)"].astype(np.float64)

        # create a new dataframe with only the alpha-3 code column from the countries_territory_dataframe
        # only has the alpha-3 code column
        country_codes_df = df_m49[["Alpha-3 code"]].copy()
        logger.info("Processed country_territory_groups.json")

        # create a list of tasks to download the country data for each country code and wait for all tasks to complete
        tasks = []
        for index, row in country_codes_df.iterrows():
            row = country_codes_df.iloc[index]
            logger.info(
                f"Downloading {row['Alpha-3 code']} from {source_url + row['Alpha-3 code'].lower()}"
            )
            text_response_task = asyncio.create_task(
                simple_url_get(os.path.join(source_url, row["Alpha-3 code"].lower()))
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
                raise Exception(
                    f"Error occurred while downloading {source_url}: {response}"
                )

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
            text_response = await simple_url_get(params_url)
            region_dataframe = pd.read_csv(
                io.StringIO(text_response[0].decode("utf-8"))
            )
            selected_dataframe = region_dataframe[
                region_dataframe["iso3"].isin(params_codes.split("|"))
            ]
            selected_dataframe["iso3"] = selected_dataframe["country"]
            selected_dataframe.rename(
                columns={"iso3": STANDARD_KEY_COLUMN}, inplace=True
            )
            selected_dataframe = selected_dataframe[country_codes_df.columns]

            country_codes_df = pd.merge(
                country_codes_df, selected_dataframe, on=STANDARD_KEY_COLUMN
            )
            # country_codes_df = pd.concat(
            #     [country_codes_df, selected_dataframe], ignore_index=True
            # )

        # create a csv file from the country_codes_df dataframe and return the csv file as bytes
        csv_data = country_codes_df.to_csv(index=False).encode("utf-8")
        logger.info(f"Successfully downloaded {source_id}")
        return csv_data, "text/csv"

    except Exception as e:
        raise e
