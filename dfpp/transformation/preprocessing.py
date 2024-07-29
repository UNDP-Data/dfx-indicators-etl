import io
import json
import logging
import os
import re
import time
from datetime import datetime

import numpy as np
import pandas as pd

from ..constants import STANDARD_COUNTRY_COLUMN, STANDARD_KEY_COLUMN
from ..storage import StorageManager
from ..utils import (
    add_country_code,
    add_region_code,
    change_iso3_to_system_region_iso3,
    fix_iso_country_codes,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


async def acctoi_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the ACCTOI transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the ACCTOI transform.

    """

    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"

    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel data into a DataFrame
        source_df = pd.read_excel(
            io.BytesIO(bytes_data), sheet_name="INFRASTRUCTURE II EN"
        )

        # Select the relevant rows from the DataFrame
        source_df = source_df.iloc[7:124]

        # Replace special characters with NaN values
        source_df = source_df.replace(["…", "-", "x[16]"], [np.nan, np.nan, np.nan])

        # Rename the column "Unnamed: 0" to "Country"
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )

        return source_df
    except Exception as e:
        logger.error(
            f"Error in acctoi_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def bti_project_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the BTI project transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the BTI project transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel data into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data))

        # Select the relevant rows from the DataFrame
        source_df = source_df.iloc[6:124]

        # Replace special characters with NaN values
        source_df = source_df.replace(["…", "-", "x[16]"], [np.nan, np.nan, np.nan])

        country_col_name = "Regions:\n1 | East-Central and Southeast Europe\n2 | Latin America and the Caribbean\n3 | West and Central Africa\n4 | Middle East and North Africa\n5 | Southern and Eastern Africa\n6 | Post-Soviet Eurasia\n7 | Asia and Oceania"
        # Rename the column with long name to "Country"
        source_df.rename(
            columns={
                country_col_name: "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        return source_df
    except Exception as e:
        logger.error(
            f"Error in bti_project_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def cpi_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the CPI transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the CPI transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel data into a DataFrame
        source_df = pd.read_json(io.BytesIO(bytes_data))

        # Rename the columns
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
                kwargs.get("year_column"): "Year",
            },
            inplace=True,
        )

        # Convert the "Year" column to datetime format
        source_df["year"] = source_df["year"].apply(
            lambda x: datetime.strptime(str(x), "%Y")
        )
        return source_df
    except Exception as e:
        logger.error(
            f"Error in cpi_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def cpia_rlpr_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the CPIA RLPR transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the CPIA RLPR transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV data into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))

        # Replace ".." with NaN values
        source_df.replace("..", np.nan, inplace=True)

        # Rename the columns
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        year_columns_pattern = r"\d{4} \[YR\d{4}\]"
        year_columns = [
            column
            for column in source_df.columns
            if re.match(year_columns_pattern, column)
        ]
        rename_columns = {column: column.split(" ")[0] for column in year_columns}
        source_df.rename(columns=rename_columns, inplace=True)
        return source_df
    except Exception as e:
        logger.error(
            f"Error in cpia_rlpr_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def cpia_spca_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the CPIA SPCA transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the CPIA SPCA transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV data into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))

        # Rename the columns
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )

        return source_df
    except Exception as e:
        logger.error(
            f"Error in cpia_spca_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def cpia_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the CPIA transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the CPIA transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")

        # Read the CSV data into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data), header=2)

        # Replace ".." with NaN values
        source_df.replace("..", np.nan, inplace=True)

        # Rename the columns
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )

        return source_df
    except Exception as e:
        logger.error(
            f"Error in cpia_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def cw_ndc_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the CW NDC transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the CW NDC transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        source_df = pd.read_csv(io.BytesIO(bytes_data))

        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )

        def reorganize_number_ranges(text):
            # Reorganize number ranges in the text to have consistent formatting
            pattern = re.compile(r"(-?\d+)-(-?\d+)")
            match = re.findall(pattern, text)
            if match:
                for m in match:
                    text = text.replace(f"{m[0]}-{m[1]}", f"{m[0]} - {m[1]}")
            else:
                pass
            return text

        def extract_minimum_number(text):
            # Extract the minimum number from the text
            numbers = re.findall(r"-?\d+\.?\d*", text)
            numbers = [float(x) for x in numbers]
            if numbers:
                min_num = min(numbers)
                return min_num
            else:
                return text

        def extract_number_with_suffix(text):
            # Extract the number with suffix " Gg" from the text
            numbers = re.findall(r"-?\d+\.?\d* Gg", text)
            if len(numbers) == 1:
                return numbers[0] + "CO"
            else:
                return text

        def get_number_count(text):
            # Get the count of numbers in the text
            return len(re.findall(r"-?\d+\.?\d*", text))

        def reorganize_floating_numbers(text):
            # Reorganize floating point numbers in the text to have consistent formatting
            return re.sub(r"(?<!\d)\.(\d+)", r"0.\1", text)

        def extract_and_change(text):
            # Extract the number and apply changes to the text based on the unit
            numbers = re.findall(r"-?\d+\.?\d*", text)
            if numbers:
                number = float(numbers[0])
                if ("ktGgCO" in text) or ("GgCO" in text):
                    number /= 10**3
                else:
                    pass
                return number
            else:
                return text

        replacement_string_values = {
            ",": "",
            "MtCO2eq": "",
            "MtCO2e": "",
            "(Mt CO2-e)2": "",
            "Mt CO2": "",
            "kt CO2eq": "ktGgCO",
        }
        # ****clean column values - M_TarA2****
        # replace n/a to empty string
        source_df.replace("n/a", "", inplace=True)
        # drop the row if either one of column values is empty
        source_df.dropna(subset=["M_TarA2", "M_TarA3", "M_TarYr"], inplace=True)
        # drop the row if either one of the columns contains sub string "Not Specified"
        source_df = source_df[~source_df["M_TarA3"].str.contains("Not Specified")]
        source_df = source_df[~source_df["M_TarA2"].str.contains("Not Specified")]

        source_df["M_TarA2"] = source_df["M_TarA2"].str.replace("%", "")
        # 26-28 -> 26 - 28
        # because 28 will be minus if this transformation is not done
        source_df["M_TarA2"] = source_df["M_TarA2"].apply(reorganize_number_ranges)
        # If there are multiple values in M_TarA2 take the lower one
        source_df["M_TarA2"] = source_df["M_TarA2"].apply(extract_minimum_number)
        # ****clean column values - M_TarA3****
        for key, value in replacement_string_values.items():
            source_df["M_TarA3"] = source_df["M_TarA3"].str.replace(
                key, value, regex=False
            )
        # extract the numbers that has suffix " Gg"
        source_df["M_TarA3"] = source_df["M_TarA3"].apply(extract_number_with_suffix)
        # calculate how many numbers are in the "M_TarA3" column cells because we drop the rows that the number count is more than 1
        source_df["M_TarA3 Number Count"] = source_df["M_TarA3"].apply(get_number_count)

        source_df = source_df[source_df["M_TarA3 Number Count"] == 1]
        # replace .23 with 0.23 otherwise it will recognize as number 23
        source_df["M_TarA3"] = source_df["M_TarA3"].apply(reorganize_floating_numbers)
        # extract the number and apply changes if the unit is not the standard one
        source_df["M_TarA3"] = source_df["M_TarA3"].apply(extract_and_change)
        source_df["M_TarA2"] = source_df["M_TarA2"].astype(float)
        source_df["M_TarA3"] = source_df["M_TarA3"].astype(float)

        source_df["NDC"] = source_df["M_TarA3"] * (1 + (source_df["M_TarA2"] / 100.0))
        source_df["ndc_date"] = pd.to_datetime(source_df["ndc_date"], format="%m/%d/%Y")
        source_df["M_TarYr"] = pd.to_datetime(source_df["M_TarYr"], format="%Y")
        # filter latest values
        source_df.sort_values("ndc_date", inplace=True, ascending=True)
        source_df.drop_duplicates(subset=["Alpha-3 code"], keep="last", inplace=True)
        return source_df
    except Exception as e:
        logger.error(
            f"Error in cw_ndc_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def cw_t2_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the CW T2 transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the CW T2 transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV data into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))

        # rename the country and key columns based on the kwargs
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Return the DataFrame without any further preprocessing
        return source_df
    except Exception as e:
        logger.error(
            f"Error in cw_t2_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def eb_wbdb_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the EB WBDB transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the EB WBDB transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    global df
    indicator_name = "Ease of doing business score"
    assert indicator_name is not None, f"indicator_id is not set in kwargs"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV data into a DataFrame
        source_df = pd.read_excel(
            io.BytesIO(bytes_data), sheet_name="DB Data (As of DB20)", header=3
        )
        year_col_mapping = {
            2016: "(DB17-20 methodology)",
            2014: "(DB15 methodology)",
            2010: "(DB10-14 methodology)",
        }
        group_df = source_df.groupby("DB Year")

        group_column_mapping = {}
        for year in group_df.groups.keys():
            for db_year in year_col_mapping.keys():
                if int(year) >= db_year:
                    indicator_suffix = year_col_mapping[db_year]
                    if (
                        " ".join([indicator_name, indicator_suffix])
                        not in group_column_mapping.keys()
                    ):
                        group_column_mapping[
                            " ".join([indicator_name, indicator_suffix])
                        ] = []
                    group_column_mapping[
                        " ".join([indicator_name, indicator_suffix])
                    ].append(year)
                    break

            for key in group_column_mapping.keys():
                df = pd.concat(
                    group_df.get_group(year) for year in group_column_mapping[key]
                )
                df["DB Year"] = df["DB Year"].apply(
                    lambda x: datetime.strptime(str(int(float(x))), "%Y")
                )
        # Return the preprocessed DataFrame
        pd.set_option("display.max_columns", None)
        df = df[
            [
                "Country code",
                "Economy",
                "Ease of doing business score (DB17-20 methodology)",
                "Ease of doing business score (DB15 methodology)",
                "Ease of doing business score (DB10-14 methodology)",
            ]
        ]
        df.rename(
            columns={
                "Ease of doing business score (DB17-20 methodology)": "2016",
                "Ease of doing business score (DB15 methodology)": "2014",
                "Ease of doing business score (DB10-14 methodology)": "2010",
            },
            inplace=True,
        )
        return df
    except Exception as e:
        logger.error(
            f"Error in eb_wbdb_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def fao_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the FAO transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the FAO transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel data into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="Data")
        # source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
        #                  inplace=True)
        # Extract the year from the "Time_Detail" column
        source_df["Time_Detail"] = source_df["Time_Detail"].apply(
            lambda x: str(x).rsplit("-")[0]
        )
        source_df["Time_Detail"] = source_df["Time_Detail"].apply(
            lambda x: datetime.strptime(str(x), "%Y")
        )
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in fao_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def ff_dc_ce_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the FF-DC-CE transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the FF-DC-CE transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel data into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="Data")
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Convert the "TimePeriod" column to datetime format
        # source_df["TimePeriod"] = pd.to_datetime(source_df["TimePeriod"], format='%Y')
        source_df["TimePeriod"] = source_df["TimePeriod"].apply(
            lambda x: datetime.strptime(str(x), "%Y")
        )
        # Filter the DataFrame to include only rows with "Type of renewable technology" as "All renewables"
        source_df = source_df[
            source_df["Type of renewable technology"] == "All renewables"
        ]
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in ff_dc_ce_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def ghg_ndc_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the GHG-NDC transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the GHG-NDC transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV data into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Filter the DataFrame to include only rows with "Sector" as "Total including LUCF" and "Gas" as "All GHG"
        source_df = source_df[
            (source_df["Sector"] == "Total including LUCF")
            & (source_df["Gas"] == "All GHG")
        ]

        # Call the change_iso3_to_system_region_iso3 function to perform additional preprocessing on the DataFrame
        source_df = await change_iso3_to_system_region_iso3(source_df, "Alpha-3 code")
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in ghg_ndc_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def gii_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the GII transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the GII transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV data into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Return the DataFrame without any preprocessing
        return source_df
    except Exception as e:
        logger.error(
            f"Error in gii_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def global_data_fsi_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the Global Data FSI transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the Global Data FSI transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV data into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Return the DataFrame without any preprocessing
        return source_df
    except Exception as e:
        logger.error(
            f"Error in global_data_fsi_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def global_findex_database_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the Global Findex Database transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the Global Findex Database transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel data into a DataFrame
        source_df = pd.read_excel(
            io.BytesIO(bytes_data), sheet_name="Data", header=0, skiprows=0
        )
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )

        source_df.rename(
            columns=lambda x: x.replace("%", "percentage").replace("+", "_plus"),
            inplace=True,
        )
        # Convert "Year" column to datetime format
        source_df["Year"] = pd.to_datetime(source_df["Year"], format="%Y")
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in global_findex_database_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def global_pi_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the Global Political Institutions transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the Global Political Institutions transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV data into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Return the DataFrame without any additional preprocessing
        return source_df
    except Exception as e:
        logger.error(
            f"Error in global_pi_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def eil_pe_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the EIL PE transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the EIL PE transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(
            io.BytesIO(bytes_data), sheet_name="7.3 Official indicator", skiprows=2
        )
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Drop rows with missing values ("..")
        source_df.drop(source_df[source_df["Value"] == ".."].index, inplace=True)

        # Reset the index
        source_df.reset_index(inplace=True)

        # Convert the "TimePeriod" column to datetime format
        source_df["TimePeriod"] = pd.to_datetime(source_df["TimePeriod"], format="%Y")
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in eil_pe_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def ec_edu_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the EC EDU transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the EC EDU transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame, skipping rows and selecting columns
        source_df = pd.read_excel(
            io.BytesIO(bytes_data),
            skiprows=10,
            nrows=202,
            usecols=[i for i in range(0, 12)],
        )
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Define the new column names
        new_column_names = [
            "Country",
            "Total",
            "Footnote_total",
            "Male",
            "fn_male",
            "Female",
            "fn_female",
            "Poorest",
            "fn_poorest",
            "Richest",
            "fn_richest",
            "Source",
        ]

        # Rename the columns
        source_df.columns = new_column_names
        # Drop rows with missing values in the "Source" column
        source_df.dropna(subset=["Source"], inplace=True)

        # Replace "-" with NaN values
        source_df.replace("-", np.nan, inplace=True)

        # Drop rows with "Source" value of "MICS2"
        source_df.drop(source_df[source_df["Source"] == "MICS2"].index, inplace=True)

        # Reset the index
        source_df.reset_index(inplace=True)

        # Extract the year from the "Source" column and create a new "Year" column
        year = []
        for i in source_df["Source"]:
            yr = re.findall(r"\d{4}", i)
            year.append(yr[0])
        source_df["Year"] = year
        source_df["Year"] = pd.to_datetime(source_df["Year"], format="%Y")
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in ec_edu_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def hdr_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the HDR transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the HDR transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        source_df = pd.read_csv(io.BytesIO(bytes_data), encoding="latin1")
        source_df.rename(columns={"iso3": "Alpha-3 code"}, inplace=True)
        source_df.set_index("Alpha-3 code", inplace=True)
        rows_to_change_mapping = {
            "ZZA.VHHD": "VHHD",
            "ZZB.HHD": "HHD",
            "ZZC.MHD": "MHD",
            "ZZD.LHD": "LHD",
            "ZZE.AS": "UNDP_AS",
            "ZZF.EAP": "UNDP_EAP",
            "ZZG.ECA": "UNDP_ECA",
            "ZZH.LAC": "UNDP_LAC",
            "ZZI.SA": "UNDP_SA",
            "ZZJ.SSA": "UNDP_SSA",
            "ZZK.WORLD": "WLD",
        }
        source_df.index = source_df.index.map(
            lambda x: rows_to_change_mapping.get(x, x)
        )
        source_df.reset_index(inplace=True)
        source_df = await change_iso3_to_system_region_iso3(source_df, "Alpha-3 code")
        source_df.reset_index(inplace=True)
        await fix_iso_country_codes(df=source_df, col="Alpha-3 code", source_id="HDR")
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        return source_df
    except Exception as e:
        logger.error(
            f"Error in hdr_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def heritage_id_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the Heritage Index of Economic Freedom transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the Heritage Index of Economic Freedom transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data))

        # Rename columns
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )

        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in heritage_id_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def ilo_ee_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the ILO Employment and Earnings transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the ILO Employment and Earnings transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV file into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))

        # Replace "none" values with NaN
        source_df = source_df.replace("none", np.nan)

        # Convert the "Year" column to datetime format
        source_df["Year"] = source_df["Year"].apply(
            lambda x: datetime.strptime(str(x), "%Y")
        )
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in ilo_ee_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def ilo_piena_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        source_df = pd.read_csv(io.BytesIO(bytes_data))

        source_df = source_df[source_df["SEX"] == kwargs.get("filter_sex_column")]
        source_df = source_df[source_df["ECO"] == kwargs.get("filter_value_column")]
        # Convert the "Time" column to datetime format
        source_df["TIME_PERIOD"] = source_df["TIME_PERIOD"].apply(
            lambda x: datetime.strptime(str(x), "%Y")
        )
        return source_df
    except Exception as e:
        logger.error(
            f"Error in ilo_piena_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def ilo_lfs_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the ILO Labor Force Survey (LFS) transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the ILO LFS transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame, skipping the first 5 rows
        source_df = pd.read_csv(io.BytesIO(bytes_data))

        source_df = source_df[
            source_df["FREQ"] == kwargs.get("filter_frequency_column")
        ]
        source_df = source_df[source_df["SEX"] == kwargs.get("filter_sex_column")]

        # Convert the "Time" column to datetime format
        source_df["TIME_PERIOD"] = source_df["TIME_PERIOD"].apply(
            lambda x: datetime.strptime(str(x), "%Y")
        )

        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in ilo_lfs_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def ilo_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the ILO transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the file. Defaults to None.
        **kwargs: Additional keyword arguments for processing.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame.
    """
    assert isinstance(bytes_data, bytes), "bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")

        # Read the CSV file into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data), low_memory=False)

        # Filter based on provided column names and values
        filters = {
            "FREQ": kwargs.get("filter_frequency_column"),
            "SEX": kwargs.get("filter_sex_column"),
            "AGE": kwargs.get("filter_age_column"),
            "ECO": kwargs.get("filter_eco_column"),
            "STE": kwargs.get("filter_ste_column"),
        }

        for col, value in filters.items():
            if value is not None:
                source_df = source_df[source_df[col] == value]
        # Convert TIME_PERIOD to datetime
        source_df["TIME_PERIOD"] = source_df["TIME_PERIOD"].apply(
            lambda x: datetime.strptime(str(x), "%Y")
        )

        # Rename columns
        source_df.rename(
            columns={kwargs.get("key_column"): "Alpha-3 code"}, inplace=True
        )

        return source_df

    except Exception as e:
        logger.error(
            f"Error in ilo_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def imf_weo_baseline_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the IMF World Economic Outlook (WEO) baseline transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the IMF WEO baseline transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data))

        # Replace "--" values with NaN
        source_df.replace("--", np.nan, inplace=True)

        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in imf_weo_baseline_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def imf_weo_gdp_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    :param bytes_data:
    :param kwargs:
    :return:
    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data))
        source_df.replace("--", np.nan, inplace=True)
        return source_df
    except Exception as e:
        logger.error(
            f"Error in imf_weo_gdp_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def imf_weo_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the IMF World Economic Outlook (WEO) transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the IMF WEO transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"

    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data))
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Replace "--" values with NaN
        source_df.replace("--", np.nan, inplace=True)

        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in imf_weo_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def iec_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the International Energy Council (IEC) transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the IEC transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data), header=1)
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Drop columns with all NaN values
        source_df.dropna(axis=1, how="all", inplace=True)

        # Assign new column names
        new_column_names = ["Country", "Energy Type", "On/Off grid", "Year", "Value"]
        source_df.columns = new_column_names

        # Forward fill NaN values in the DataFrame
        source_df = source_df.fillna(method="ffill")

        # Drop rows where the value is ".."
        source_df.drop(source_df[source_df["Value"] == ".."].index, inplace=True)

        # Convert the "Year" column to datetime format
        source_df["Year"] = pd.to_datetime(source_df["Year"], format="%Y")

        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in iec_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def imsmy_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the International Monetary Statistics (IMSMY) transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the IMSMY transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(
            io.BytesIO(bytes_data), sheet_name="Table 1", header=10
        )
        # Replace ".." with NaN values
        source_df.replace("..", np.nan, inplace=True)

        # Rename the "Region, development group, country or area" column to "Country"

        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )

        # Remove spaces and asterisks from the "Country" column values
        source_df["Country"] = source_df["Country"].apply(
            lambda x: x.replace(" ", "").replace("*", "")
        )

        # Convert the "Year" column to datetime format
        source_df["Year"] = source_df["Year"].apply(
            lambda x: datetime.strptime(str(x), "%Y")
        )

        # Add country codes to the DataFrame
        source_df = await add_country_code(source_df, "Country")
        # Add region codes to the DataFrame
        source_df = await add_region_code(source_df, "Country")

        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in imsmy_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def inequality_hdi_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the Inequality and Human Development Index (HDI) transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the Inequality and HDI transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(
            io.BytesIO(bytes_data), sheet_name="Table 3", header=4
        )
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Select the desired rows (from row 1 to 200)
        source_df = source_df.iloc[1:200]

        # Replace ".." with NaN values
        source_df = source_df.replace("..", np.nan)

        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in inequality_hdi_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def isabo_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the ISABO transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the JSON file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the ISABO transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the JSON file into a DataFrame
        source_df = pd.read_json(io.BytesIO(bytes_data))

        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )

        # Flatten the "table1" column into separate columns
        source_df = pd.json_normalize(source_df["table1"])

        # Convert the "year" column to float
        source_df["year"] = source_df["year"].astype(float)

        # Drop rows with missing values in the "year" column
        source_df.dropna(inplace=True, subset=["year"])

        # Replace "NaN" strings with NaN values
        source_df.replace("NaN", np.nan, inplace=True)

        # Convert the "year" column to datetime format
        source_df["year"] = source_df["year"].apply(
            lambda x: datetime.strptime(str(round(x)), "%Y")
        )

        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in isabo_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def itu_ict_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the ITU ICT transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the ITU ICT transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="i99H")
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in itu_ict_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def mdp_bpl_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the MDP BPL transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the MDP BPL transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data))
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Remove the first row from the DataFrame
        source_df = source_df.iloc[1:]
        # Extract the country name from the "countryname" column
        source_df["Country"] = source_df["Country"].apply(
            lambda x: " ".join(x.rsplit(" ")[1:])
        )
        # Extract the year from the "period" column
        source_df["period"] = source_df["period"].apply(
            lambda x: datetime.strptime(str(x).rsplit(" ")[-1], "%Y")
        )
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in mdp_bpl_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


# async def mdp_indicators_taf_transform_preprocessing(bytes_data: bytes= None):
#     source_df = pd.read_json(io.BytesIO(bytes_data))
#     source_df


async def mdp_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the MDP transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the JSON file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the MDP transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")

        async def mdp_metadata():
            async with StorageManager() as storage:
                country_df_bytes = await storage.download(
                    blob_name=os.path.join(
                        os.environ.get("ROOT_FOLDER"),
                        "config",
                        "utilities",
                        "MDP_META.json",
                    )
                )
                return country_df_bytes

        # Read the country metadata JSON file into a DataFrame
        country_df = pd.read_json(io.BytesIO(await mdp_metadata()))

        # Extract the required columns from the country DataFrame
        country_df = country_df[["id", "name"]]

        # Set the "id" column as the index of the country DataFrame
        country_df.set_index("id", inplace=True)

        # Read the MDP data JSON file into a DataFrame
        source_df = pd.read_json(io.BytesIO(bytes_data))

        # Extract the required columns from the MDP DataFrame
        source_df = source_df[["c", "v", "t"]]

        # Set the "c" column as the index of the MDP DataFrame
        source_df.set_index("c", inplace=True)

        # Join the MDP DataFrame with the country DataFrame using the index
        source_df = source_df.join(country_df)

        # Reset the index of the MDP DataFrame
        source_df.reset_index(inplace=True)

        # Convert the "t" column to datetime format
        source_df["t"] = source_df["t"].apply(lambda x: datetime.strptime(str(x), "%Y"))
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in mdp_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def natural_capital_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the natural capital transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the natural capital transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data))
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Calculate the total natural capital by summing the renewable and nonrenewable natural capital
        source_df["Total"] = (
            source_df["Renewable natural capital"]
            + source_df["Nonrenewable natural capital"]
        )

        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in natural_capital_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def nature_co2_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the nature CO2 transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the nature CO2 transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data), header=1)
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Convert a column to percentages
        source_df["percent.6"] = source_df["percent.6"].apply(lambda x: x * 100)

        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in nature_co2_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def nd_climate_readiness_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the climate readiness transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the climate readiness transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV file into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Define the non-value columns
        non_values_columns = ["ISO3", "Name"]

        # Add rank columns for each non-value column
        for column_name in source_df.columns:
            if column_name in non_values_columns:
                continue
            else:
                source_df[column_name + "_rank"] = source_df[column_name].rank(
                    method="dense", ascending=False
                )

        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in nd_climate_readiness_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def oecd_raw_mat_consumption_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the OECD raw material consumption transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the OECD raw material consumption transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV file into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Drop rows with missing values in the 'Value' column
        source_df = source_df.dropna(subset=["Value"])

        # Convert the 'TIME' column to datetime format
        source_df["TIME"] = source_df["TIME"].apply(
            lambda x: datetime.strptime(str(x), "%Y")
        )

        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in oecd_raw_mat_consumption_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def owid_energy_data_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the OWID energy data transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the OWID energy data transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV file into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))

        # Drop rows with missing values in the 'year' column
        source_df.dropna(subset=["year"], inplace=True)

        # Convert the 'year' column to datetime format
        source_df["year"] = source_df["year"].apply(
            lambda x: datetime.strptime(str(round(float(x))), "%Y")
        )

        # Add region codes to the DataFrame
        source_df = await add_region_code(source_df, "country", "iso_code")
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in owid_energy_data_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def owid_export_transform(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the OWID export transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the OWID export transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV file into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))

        # Drop rows with missing values in the 'Year' column
        source_df.dropna(subset=["Year"], inplace=True)

        # Convert the 'Year' column to datetime format
        source_df["Year"] = source_df["Year"].apply(
            lambda x: datetime.strptime(str(round(float(x))), "%Y")
        )

        # Add region codes to the DataFrame
        source_df = await add_region_code(source_df, "Entity", "Code")
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(f"Error in owid_export_transform: {e}")
        raise e


async def owid_oz_consumption_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the OWID ozone consumption transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the OWID ozone consumption transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV file into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))

        # Define the column names for summation
        summation_column_names = [
            "Consumption of controlled substance (zero-filled) - Chemical: Methyl Chloroform (TCA)",
            "Consumption of controlled substance (zero-filled) - Chemical: Methyl Bromide (MB)",
            "Consumption of controlled substance (zero-filled) - Chemical: Hydrochlorofluorocarbons (HCFCs)",
            "Consumption of controlled substance (zero-filled) - Chemical: Carbon Tetrachloride (CTC)",
            "Consumption of controlled substance (zero-filled) - Chemical: Halons",
            "Consumption of controlled substance (zero-filled) - Chemical: Chlorofluorocarbons (CFCs)",
        ]

        # Convert the 'Year' column to datetime format
        source_df["Year"] = pd.to_datetime(source_df["Year"], format="%Y")

        # Calculate the sum of the specified columns and create a new 'Chemical Total' column
        source_df["Chemical Total"] = source_df[summation_column_names].sum(axis=1)
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in owid_oz_consumption_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def owid_t3_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the OWID T3 transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the OWID T3 transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Mapping dictionaries for column names and formats
        indicator_mapping = {
            "pollution_deaths_fossil_fuels_owid": "Excess mortality from fossil fuels",
        }
        time_column_mapping = {
            "OWID_AIR": "Year",
        }
        time_format_mapping = {
            "OWID_AIR": "%Y",
        }
        country_name_mapping = {
            "OWID_AIR": "Entity",
        }
        country_iso3_mapping = {
            "OWID_AIR": "Code",
        }

        # Read the CSV file into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))

        # Drop rows with missing values in the time column
        source_df.dropna(subset=["Year"], inplace=True)

        # Convert the time column to datetime format
        source_df["Year"] = pd.to_datetime(source_df["Year"], format="%Y")
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in owid_t3_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def owid_trade_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the OWID trade transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the OWID trade transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")

        # Read the CSV file into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))
        country_col_name = "Entity"
        iso_col_name = "Code"

        # Drop rows with missing values in the Year column
        source_df.dropna(subset=["Year"], inplace=True)
        source_df["Year"] = source_df["Year"].apply(
            lambda x: datetime.strptime(str(round(float(x))), "%Y")
        )

        # Add region code to the DataFrame using the add_region_code function
        source_df = await add_region_code(
            source_df=source_df,
            region_name_col=country_col_name,
            region_key_col=iso_col_name,
        )
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in owid_trade_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def oxcgrt_rl_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the OxCGRT RL transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the OxCGRT RL transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV file into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))

        # Convert the "Date" column to datetime format
        source_df["Date"] = source_df["Date"].apply(
            lambda x: datetime.strptime(str(x), "%Y%m%d")
        )

        # Update the "StringencyIndex_Average" column with values from "StringencyIndex_Average_ForDisplay"
        source_df["StringencyIndex_Average_ForDisplay"].update(
            source_df["StringencyIndex_Average"]
        )
        source_df["StringencyIndex_Average"] = source_df[
            "StringencyIndex_Average_ForDisplay"
        ]
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in oxcgrt_rl_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def pts_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the PTS transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the PTS transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV file into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))

        # Convert the "Year" column to datetime format
        source_df["Year"] = source_df["Year"].apply(
            lambda x: datetime.strptime(str(x), "%Y")
        )

        # Replace "NA" values with NaN
        source_df.replace("NA", np.nan, inplace=True)
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in pts_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def sdg_mr_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the SDG MR transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the SDG MR transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="Table format")
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in sdg_mr_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def sdg_rap_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the SDG RAP transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the SDG RAP transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))

        # Convert the "Time" column to datetime format
        source_df["TIME_PERIOD"] = source_df["TIME_PERIOD"].apply(
            lambda x: datetime.strptime(str(x), "%Y")
        )
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # print(source_df.columns.tolist())
        # exit()
        source_df = source_df[source_df["SOC"] == kwargs.get("filter_value_column")]
        source_df = source_df[source_df["SEX"] == kwargs.get("filter_sex_column")]
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in sdg_mr_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def sipri_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the SIPRI transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the SIPRI transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(
            io.BytesIO(bytes_data), sheet_name="Share of Govt. spending", header=7
        )

        # Replace "..." with NaN values
        source_df.replace(["...", "xxx"], [np.nan, np.nan], inplace=True)
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in sipri_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def undp_gii_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the UNDP GII transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the UNDP GII transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data), header=5)

        # Select the relevant rows
        source_df = source_df.iloc[2:202]

        # Replace ".." with NaN values
        source_df.replace("..", np.nan, inplace=True)
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in undp_gii_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def undp_hdi_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the UNDP HDI transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the UNDP HDI transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV file into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in undp_hdi_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def undp_mpi_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the UNDP MPI transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the UNDP MPI transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame (Table 2)
        source_df = pd.read_excel(
            io.BytesIO(bytes_data), header=4, sheet_name="Table 2"
        )

        # Filter rows and columns
        source_df = source_df.iloc[:188]

        # Read the Excel file into a DataFrame (Table 1 for region data)
        region_source_df = pd.read_excel(
            io.BytesIO(bytes_data), header=4, sheet_name="Table 1"
        )

        # Extract the relevant region data
        start = region_source_df[
            region_source_df["Country"] == "Developing countries"
        ].index[0]
        end = region_source_df[region_source_df["Country"] == "Notes"].index[0]
        region_source_df = region_source_df.iloc[start : end - 2]
        region_source_df_sub = region_source_df[["Country", "Value"]]
        region_source_df_sub.rename(columns={"Country": "Unnamed: 0"}, inplace=True)

        # Extract the year from column name and append region data to source_df
        region_year = region_source_df.columns[2].split("-")[-1]
        region_source_df_sub["Unnamed: 2"] = region_year
        source_df = pd.concat([source_df, region_source_df_sub], axis=0)

        # Preprocess columns
        source_df["Unnamed: 2"] = source_df["Unnamed: 2"].apply(
            lambda x: x.rsplit(" ")[0].rsplit("/")[-1]
        )
        source_df["Unnamed: 2"] = source_df["Unnamed: 2"].apply(
            lambda x: datetime.strptime(str(x), "%Y")
        )
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in undp_mpi_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def unescwa_fr_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the UNESCWA FR transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the UNESCWA FR transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="Sheet1")

        # Multiply the 'Government fiscal support (Bn USD) 2020 & 2021' column by (10 ** 9)
        source_df["Government fiscal support (Bn USD) 2020 & 2021"] = source_df[
            "Government fiscal support (Bn USD) 2020 & 2021"
        ].apply(lambda x: x * (10**9))
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in unescwa_fr_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def unicef_dev_ontrk_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the UNICEF Development On-Track transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the UNICEF Development On-Track transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV file into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))

        # Convert the 'TIME_PERIOD' column to datetime format
        source_df["TIME_PERIOD"] = source_df["TIME_PERIOD"].apply(
            lambda x: datetime.strptime(str(x), "%Y")
        )

        # Replace 'No data' values with NaN
        source_df.replace(["No data"], [np.nan], inplace=True)

        # Perform additional preprocessing if required, e.g., changing ISO3 codes to system region ISO3 codes
        source_df = await change_iso3_to_system_region_iso3(source_df, "REF_AREA")
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in unicef_dev_ontrk_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def untp_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the United Nations Population Estimates transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the United Nations Population Estimates transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))

        # Drop rows with missing values in the 'Year' column
        source_df.dropna(subset=["Time"], inplace=True)
        start_year = 1980
        current_year = int(time.strftime("%Y"))
        source_df.loc[source_df["LocTypeName"] == "World", "ISO3_code"] = "WLD"
        source_df = source_df[
            (source_df["Time"] >= start_year) & (source_df["Time"] <= current_year)
        ]
        source_df["Time"] = source_df["Time"].apply(
            lambda x: datetime.strptime(str(round(float(x))), "%Y")
        )
        source_df["TPopulation1July"] = source_df["TPopulation1July"].apply(
            lambda x: x * 1000
        )
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in untp_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def vdem_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the Varieties of Democracy transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the Varieties of Democracy transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV file into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))

        # Convert the values in the 'year' column to datetime format
        source_df["year"] = source_df["year"].apply(
            lambda x: datetime.strptime(str(x), "%Y")
        )
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in vdem_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def tech_ict_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")

        source_df = pd.read_csv(io.BytesIO(bytes_data), encoding="ISO-8859-1", header=1)

        rename_columns = {column: column.strip() for column in source_df.columns}

        source_df.rename(columns=rename_columns, inplace=True)
        datetime_column = kwargs.get("datetime_column")

        # Convert the values in the 'Period' column to datetime format
        source_df[datetime_column] = pd.to_datetime(
            source_df[datetime_column], format="%Y"
        )
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in tech_ict_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def time_udc_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the Time UDC transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the Time UDC transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV file into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data), encoding="ISO-8859-1")

        datetime_column = kwargs.get("datetime_column")
        print(source_df.head())
        # Convert the values in the 'Period' column to datetime format
        source_df[datetime_column] = pd.to_datetime(
            source_df[datetime_column], format="%Y"
        )
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in time_udc_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def wbank_access_elec_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the World Bank Electricity Access transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the World Bank Electricity Access transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="Data", header=0)

        # Rename the columns to match the desired format
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        year_columns_pattern = r"\d{4} \[YR\d{4}\]"
        year_columns = [
            column
            for column in source_df.columns
            if re.match(year_columns_pattern, column)
        ]
        rename_columns = {column: column.split(" ")[0] for column in year_columns}
        source_df.rename(columns=rename_columns, inplace=True)
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in wbank_access_elec_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def wbank_info_eco_transform_preprocessing(
    bytes_data: bytes = None, sheet_name=None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the World Bank Economic Information transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.
        sheet_name (str or int, optional): The name or index of the sheet to read. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the World Bank Economic Information transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(
            io.BytesIO(bytes_data), sheet_name=sheet_name, header=0
        )
        # strip the column names in pandas dataframe
        source_df.rename(columns=lambda x: str(x).strip(), inplace=True)
        # Rename the columns to match the desired format
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )

        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in wbank_info_eco_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def wbank_info_transform_preprocessing(
    bytes_data: bytes = None, sheet_name=None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the World Bank Information transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.
        sheet_name (str or int, optional): The name or index of the sheet to read. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the World Bank Information transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    assert sheet_name is not None, "Sheet name cannot be None"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        # source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name=sheet_name, header=13)
        source_df = pd.read_csv(io.BytesIO(bytes_data), header=0, skiprows=4)
        # Create a dictionary to hold the column renaming information
        column_rename = {"Country Name": "Country", "Country Code": "Alpha-3 code"}

        # # Iterate over the columns and generate new column names
        # for column in source_df.columns:
        #     column_rename[column] = str(column) + "_" + source_df.iloc[0][column]

        # Rename the columns using the column_rename dictionary
        source_df.rename(columns=column_rename, inplace=True)

        return source_df
    except Exception as e:
        logger.error(
            f"Error in wbank_info_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def wbank_poverty_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the World Bank Poverty transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the World Bank Poverty transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data), header=0, skiprows=3)

        # Select the desired columns from the DataFrame
        source_df = source_df[["code", "economy", "mdpoor_i1", "year"]]

        # Drop rows with missing values
        source_df.dropna(inplace=True)

        # Create a dictionary to hold the column renaming information
        column_rename = {}

        # Rename the columns using the column_rename dictionary
        source_df.rename(columns=column_rename, inplace=True)

        # Convert the 'year' column datatype to a datetime object
        source_df["year"] = pd.to_datetime(source_df["year"], format="%Y")
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in wbank_poverty_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def wbank_energy_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the World Bank Energy transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the World Bank Energy transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="Data", header=0)

        # Create a dictionary to hold the column renaming information
        column_rename = {}
        i = 0

        # Iterate over the columns and assign new column names
        for column in source_df.columns:
            if i > 3:
                column_rename[column] = str(column)[:4]
            else:
                if column == "Country Name":
                    column_rename[column] = "Country"
                elif column == "Country Code":
                    column_rename[column] = "Alpha-3 code"
                else:
                    column_rename[column] = str(column)
            i = i + 1

        # Rename the columns using the column_rename dictionary
        source_df.rename(columns=column_rename, inplace=True)

        # Replace ".." with NaN values in the DataFrame
        source_df = source_df.replace("..", np.NaN)
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in wbank_energy_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def wbank_rai_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the World Bank RAI transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the World Bank RAI transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(
            io.BytesIO(bytes_data), sheet_name="RAI Crosstab", header=2
        )

        # Rename the columns with a prefix "year_" for integer columns
        source_df.columns = [
            f"year_{col}" if isinstance(col, int) else col for col in source_df.columns
        ]

        # Drop rows with all NaN values
        source_df = source_df.dropna(how="all")
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Return the preprocessed DataFrame

        # print(source_df.head())
        # exit()
        return source_df
    except Exception as e:
        logger.error(
            f"Error in wbank_rai_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def wbank_t1_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the World Bank Table 1 transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the World Bank Table 1 transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="Data", header=3)
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in wbank_t1_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def who_pre_edu_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the WHO Pre-Education Statistics transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the JSON file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the WHO Pre-Education Statistics transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the JSON data into a DataFrame
        source_df = pd.read_json(io.BytesIO(bytes_data))

        # Extract the "value" column
        source_df = source_df["value"]

        # Normalize the JSON data into a DataFrame
        source_df = pd.json_normalize(source_df)

        # Convert the "TimeDim" column to datetime format
        source_df["TimeDim"] = source_df["TimeDim"].apply(
            lambda x: datetime.strptime(str(x), "%Y")
        )

        # Pivot the DataFrame to reshape the data
        pivot_df = source_df.pivot_table(
            values="NumericValue", index=["SpatialDim", "TimeDim"], columns="Dim1"
        ).reset_index()

        # Replace 'No data' values with NaN
        pivot_df.replace(["No data"], [np.nan], inplace=True)

        # Map ISO3 codes to system region ISO3 codes
        pivot_df = await change_iso3_to_system_region_iso3(pivot_df, "SpatialDim")
        pivot_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Return the preprocessed DataFrame
        return pivot_df
    except Exception as e:
        logger.error(
            f"Error in who_pre_edu_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def who_rl_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the data for the WHO Right to Health transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the JSON file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the WHO Right to Health transform.

    """
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the JSON data into a DataFrame
        source_df = pd.read_json(io.BytesIO(bytes_data))
        # Extract the "value" column
        source_df = source_df["value"]

        # Normalize the JSON data into a DataFrame
        source_df = pd.json_normalize(source_df)

        # Replace 'No data' values with NaN
        source_df.replace(["No data"], [np.nan], inplace=True)

        # Convert the "TimeDim" column to datetime format
        source_df["TimeDim"] = source_df["TimeDim"].apply(
            lambda x: datetime.strptime(str(x), "%Y")
        )

        # Map ISO3 codes to system region ISO3 codes
        source_df = await change_iso3_to_system_region_iso3(source_df, "SpatialDim")
        source_df.rename(
            columns={
                kwargs.get("country_column"): "Country",
                kwargs.get("key_column"): "Alpha-3 code",
            },
            inplace=True,
        )
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in who_rl_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def wbdb_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the given Excel data in bytes format and returns it as a Pandas DataFrame.

    Args:
        bytes_data (bytes): The Excel data in bytes format.
        **kwargs: Additional keyword arguments.

    Returns:
        pd.DataFrame: The preprocessed DataFrame.

    Raises:
        AssertionError: If `bytes_data` argument is not of type bytes.
        Exception: If any error occurs during preprocessing.

    Example:
        bytes_data = b'...'  # Some Excel data in bytes format
        df = await wbdb_transform_preprocessing(bytes_data)
    """
    # Check if bytes_data is of type bytes
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"

    try:
        # Read Excel data into a DataFrame using pandas
        source_df = pd.read_excel(io.BytesIO(bytes_data))

        # Remove the "*" character and any following text from the "Economy" column
        source_df["Economy"] = source_df["Economy"].apply(
            lambda x: x.rsplit("*")[0] if "*" in str(x) else x
        )

        # Drop rows with missing values in the "Year" column
        source_df.dropna(subset=["Year"], inplace=True)

        # Convert the "Year" column to datetime objects, assuming it contains year values
        source_df["Year"] = source_df["Year"].apply(
            lambda x: datetime.strptime(str(int(x)), "%Y")
        )

        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        # Log the error and raise the exception again
        logger.error(
            f"Error in wbdb_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def ilo_sdg_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the given Excel data in bytes format and returns it as a Pandas DataFrame.

    Args:
        bytes_data (bytes): The Excel data in bytes format.
        **kwargs: Additional keyword arguments.

    Returns:
        pd.DataFrame: The preprocessed DataFrame.

    Raises:
        AssertionError: If `bytes_data` argument is not of type bytes.
        Exception: If any error occurs during preprocessing.

    Example:
        bytes_data = b'...'  # Some Excel data in bytes format
        df = await ilo_sdg_transform_preprocessing(bytes_data)
    """
    # Check if bytes_data is of type bytes
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"

    try:
        # Read Excel data into a DataFrame using pandas with header starting from the 6th row (0-indexed)
        source_df = pd.read_csv(io.BytesIO(bytes_data))

        # Convert the "Time" column to datetime objects, assuming it contains year values
        source_df["TIME_PERIOD"] = source_df["TIME_PERIOD"].apply(
            lambda x: datetime.strptime(str(x), "%Y")
        )

        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        # Log the error and raise the exception again
        logger.error(
            f"Error in ilo_sdg_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def ilo_spf_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"
    try:
        # source_df = pd.read_excel(io.BytesIO(bytes_data), header=5)
        source_df = pd.read_csv(io.BytesIO(bytes_data))
        source_df.rename(columns={kwargs.get("key_column"): "Alpha-3 code"})
        source_df["TIME_PERIOD"] = source_df["TIME_PERIOD"].apply(
            lambda x: datetime.strptime(str(x), "%Y")
        )
        source_df = source_df[source_df["SOC"] == kwargs.get("filter_value_column")]
        source_df = source_df[source_df["SEX"] == kwargs.get("filter_sex_column")]
        print(source_df.head())
        return source_df
    except Exception as e:
        logger.error(
            f"Error in ilo_spf_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def imf_ifi_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the given Excel data in bytes format and returns it as a Pandas DataFrame.

    Args:
        bytes_data (bytes): The Excel data in bytes format.
        **kwargs: Additional keyword arguments.

    Returns:
        pd.DataFrame: The preprocessed DataFrame.

    Raises:
        AssertionError: If `bytes_data` argument is not of type bytes.
        Exception: If any error occurs during preprocessing.

    Example:
        bytes_data = b'...'  # Some Excel data in bytes format
        df = await imf_ifi_transform_preprocessing(bytes_data)
    """
    # Check if bytes_data is of type bytes
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"

    try:
        # Read Excel data into a DataFrame using pandas for "financial assistance" sheet
        source_df = pd.read_excel(
            io.BytesIO(bytes_data),
            sheet_name="financial assistance",
            header=3,
            engine="openpyxl",
        )

        # Read Excel data into another DataFrame using pandas for "debt-relief" sheet
        source_df2 = pd.read_excel(
            io.BytesIO(bytes_data), sheet_name="debt-relief", engine="openpyxl"
        )

        # Rename columns in the second DataFrame to match the first DataFrame
        source_df2.rename(
            columns={
                "country": "Country",
                "source": "Type of Emergency Financing",
                "amount approved in SDR": "Amount Approved in SDR",
                "amount approved in USD": "Amount Approved in US$",
                "date of approval": "Date of Approval",
            },
            inplace=True,
        )

        # Concatenate the two DataFrames together
        source_df = pd.concat([source_df, source_df2], ignore_index=True)

        # Drop rows with missing values in the "Date of Approval" column
        source_df.dropna(subset=["Date of Approval"], inplace=True)

        # Convert the 'Date of Approval' column to datetime using datetime.strptime
        source_df["Date of Approval"] = source_df["Date of Approval"].apply(
            lambda x: datetime.strptime(str(x).rsplit(" ")[0], "%Y-%m-%d")
        )

        # Convert the 'Amount Approved in US$' column to numeric format
        source_df["Amount Approved in US$"] = source_df["Amount Approved in US$"].apply(
            lambda x: float(x.rsplit("mill")[0].replace(",", "").replace("US$", ""))
            * (10**6)
        )

        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        # Log the error and raise the exception again
        logger.error(
            f"Error in imf_ifi_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def who_global_rl_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the given CSV data in bytes format and returns it as a Pandas DataFrame.

    Args:
        bytes_data (bytes): The CSV data in bytes format.
        **kwargs: Additional keyword arguments.

    Returns:
        pd.DataFrame: The preprocessed DataFrame.

    Raises:
        AssertionError: If `bytes_data` argument is not of type bytes.
        Exception: If any error occurs during preprocessing.

    Example:
        bytes_data = b'...'  # Some CSV data in bytes format
        df = await who_global_rl_transform_preprocessing(bytes_data)
    """
    # Check if bytes_data is of type bytes
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"

    try:
        # Read CSV data into a DataFrame using pandas
        source_df = pd.read_csv(io.BytesIO(bytes_data))

        # Convert the "Date_reported" column to datetime objects
        source_df["Date_reported"] = source_df["Date_reported"].apply(
            lambda x: datetime.strptime(str(x), "%Y-%m-%d")
        )

        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        # Log the error and raise the exception again
        logger.error(
            f"Error in who_global_rl_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def sme_transform_preprocessing(
    bytes_data: bytes = None, **kwargs
) -> pd.DataFrame:
    """
    Preprocesses the given Excel data in bytes format and returns it as a Pandas DataFrame.

    Args:
        bytes_data (bytes): The Excel data in bytes format.
        **kwargs: Additional keyword arguments.

    Returns:
        pd.DataFrame: The preprocessed DataFrame.

    Raises:
        AssertionError: If `bytes_data` argument is not of type bytes.
        Exception: If any error occurs during preprocessing.

    Example:
        bytes_data = b'...'  # Some Excel data in bytes format
        df = await sme_transform_preprocessing(bytes_data)
    """
    # Check if bytes_data is of type bytes
    assert isinstance(bytes_data, bytes), f"bytes_data arg needs to be of type bytes"

    try:
        # Read Excel data into a DataFrame using pandas
        source_df = pd.read_excel(
            io.BytesIO(bytes_data), sheet_name="Time Series", engine="openpyxl"
        )

        # Replace ":" with NaN values in the DataFrame
        source_df.replace(":", np.nan, inplace=True)

        # Drop the first row (assuming it contains headers)
        source_df = source_df.iloc[1:]

        # Reset the index of the DataFrame
        source_df.reset_index(inplace=True)

        # Rename columns to standardized names
        source_df.rename(
            columns={
                "Country": STANDARD_COUNTRY_COLUMN,
                "Country Code": STANDARD_KEY_COLUMN,
            },
            inplace=True,
        )

        # Remove any newline characters in column names
        source_df.rename(columns=lambda x: x.replace("\n", ""), inplace=True)
        # print(source_df.columns.tolist())
        # print(source_df.head())
        # exit()
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        # Log the error and raise the exception again
        logger.error(
            f"Error in sme_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}"
        )
        raise e


async def hefpi_wdi_transform_preprocessing(
    bytes_data: bytes, **kwargs
) -> pd.DataFrame:
    source_df = pd.read_csv(io.BytesIO(bytes_data))
    source_df = source_df.loc[:, ~source_df.columns.str.contains("^Unnamed")]
    return source_df


async def std_exp_transform_preprocessing(bytes_data: bytes, **kwargs) -> pd.DataFrame:
    source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="Data")
    source_df["year"] = pd.to_datetime(source_df["year"], format="%Y")
    return source_df


async def wbank_aids_transform_preprocessing(
    bytes_data: bytes, **kwargs
) -> pd.DataFrame:
    source_df = pd.read_csv(io.BytesIO(bytes_data))
    return source_df


async def il_fishing_transform_preprocessing(
    bytes_data: bytes, **kwargs
) -> pd.DataFrame:
    async with StorageManager() as storage_manager:
        meta_bytes_data = await storage_manager.download(
            blob_name=os.path.join(
                storage_manager.ROOT_FOLDER, "sources", "raw", "IL_FISH_META.json"
            )
        )
        meta_data = meta_bytes_data.decode("utf-8")
        meta_data_dict = json.loads(meta_data)
        meta_data_dict = meta_data_dict["dimensions"]["entities"]["values"]
        source_meta_df = pd.DataFrame(meta_data_dict)
        source_data_df = pd.read_json(io.BytesIO(bytes_data))
        source_df = source_data_df.merge(
            source_meta_df, left_on="entities", right_on="id"
        )
        source_df["years"] = pd.to_datetime(source_df["years"], format="%Y")
        return source_df


async def ihr_spar_transform_preprocessing(bytes_data: bytes, **kwargs) -> pd.DataFrame:
    indicator_id = kwargs.get("indicator_id")
    async with StorageManager() as storage_manager:
        indicator_cfg = await storage_manager.get_indicator_cfg(
            indicator_id=indicator_id
        )
        source_id = indicator_cfg["indicator"]["source_id"]
        source_cfg = await storage_manager.get_source_cfg(source_id=source_id)
        file_format = source_cfg["source"]["file_format"]
        if file_format == "csv":
            source_df = pd.read_csv(io.BytesIO(bytes_data))
        elif file_format == "json":
            source_data = bytes_data.decode("utf-8")
            source_data_dict = json.loads(source_data)
            source_df = pd.DataFrame(source_data_dict["value"])
            source_df["TimeDim"] = source_df["TimeDim"].apply(
                lambda x: datetime.strptime(str(x), "%Y")
            )
        else:
            raise Exception(f"File format {file_format} not supported")
        #
        return source_df


async def unaids_aids_transform_preprocessing(
    bytes_data: bytes, **kwargs
) -> pd.DataFrame:
    source_df = pd.read_csv(io.BytesIO(bytes_data), encoding="latin-1")
    source_df = source_df[
        source_df["Indicator"]
        == "Denied health services because of their HIV status in the last 12 months"
    ]
    source_df["Time Period"] = source_df["Time Period"].apply(
        lambda x: datetime.strptime(str(x), "%Y")
    )
    return source_df


async def fao_forest_area_transform_preprocessing(
    bytes_data: bytes, **kwargs
) -> pd.DataFrame:
    source_df = pd.read_csv(io.BytesIO(bytes_data), encoding="latin1")
    source_df["Time_Detail"] = pd.to_datetime(source_df["Time_Detail"], format="%Y")
    return source_df


async def med_waste_transform_preprocessing(
    bytes_data: bytes, **kwargs
) -> pd.DataFrame:
    async with StorageManager() as storage_manager:
        meta_bytes_data = await storage_manager.download(
            blob_name=os.path.join(
                storage_manager.ROOT_FOLDER, "sources", "raw", "MED_WASTE_META.csv"
            )
        )
        source_meta_df = pd.read_csv(
            io.BytesIO(meta_bytes_data),
            encoding="ISO-8859-1",
            usecols=["iso3c", "measurement", "year"],
        )
        source_data_df = pd.read_csv(
            io.BytesIO(bytes_data),
            encoding="ISO-8859-1",
            usecols=["iso3c", "country_name", "special_waste_medical_waste_tons_year"],
        )
        source_df = source_data_df.merge(
            source_meta_df, left_on="iso3c", right_on="iso3c"
        )
        source_df["year"] = pd.to_datetime(source_df["year"], format="%Y")
        source_df.dropna(subset=["year"], inplace=True)
        print(source_df.head())
        return source_df


async def who_domestic_hiv_transform_preprocessing(
    bytes_data: bytes, **kwargs
) -> pd.DataFrame:
    source_df = pd.read_excel(io.BytesIO(bytes_data))
    source_df["year"] = source_df["year"].apply(
        lambda x: datetime.strptime(str(x), "%Y")
    )
    return source_df


async def frst_area_transform_preprocessing(
    bytes_data: bytes, **kwargs
) -> pd.DataFrame:
    source_df = pd.read_csv(io.BytesIO(bytes_data), skiprows=4)
    source_df = source_df.loc[
        :, ~source_df.columns.str.contains("^Unnamed")
    ]  # remove unnamed columns
    return source_df


async def global_health_security_index_transform_preprocessing(
    bytes_data: bytes, **kwargs
) -> pd.DataFrame:
    source_df = pd.read_csv(io.BytesIO(bytes_data))
    source_df["Year"] = source_df["Year"].apply(
        lambda x: datetime.strptime(str(x), "%Y")
    )
    return source_df


async def unaids_transform_preprocessing(bytes_data: bytes, **kwargs) -> pd.DataFrame:
    source_df = pd.read_csv(io.BytesIO(bytes_data), encoding="latin1")
    source_df = source_df[source_df["Time Period"] != "UNAIDS_TGF_Data_"]
    source_df["Time Period"] = pd.to_datetime(source_df["Time Period"], format="%Y")
    # drop columns in "Data Value" that are not numeric
    source_df["Data value"] = pd.to_numeric(source_df["Data value"], errors="coerce")
    # all values that are < 0.01 are set to 0
    source_df["Data value"] = source_df["Data value"].apply(
        lambda x: 0 if x < 0.01 else x
    )
    # print(kwargs)
    # print(source_df['data'])
    # exit()
    source_df.replace("Türkiye", "Turkey", inplace=True)
    return source_df


async def mhs_totwf_transform_preprocessing(
    bytes_data: bytes, **kwargs
) -> pd.DataFrame:
    source_df = pd.read_csv(io.BytesIO(bytes_data))
    source_df["YEAR"] = pd.to_datetime(source_df["YEAR"], format="%Y")
    return source_df


async def sdmx_unicef_transform_preprocessing(
    bytes_data: bytes, **kwargs
) -> pd.DataFrame:
    source_df = pd.read_csv(io.BytesIO(bytes_data), encoding="ISO-8859-1")
    source_df["TIME_PERIOD"] = pd.to_datetime(source_df["TIME_PERIOD"], format="%Y")
    return source_df


async def pas_dis_transform_preprocessing(bytes_data: bytes, **kwargs) -> pd.DataFrame:
    source_df = pd.read_csv(io.BytesIO(bytes_data))
    source_df["Year"] = pd.to_datetime(source_df["Year"], format="%Y")
    return source_df


async def ihme_static_transform_preprocessing(
    bytes_data: bytes, **kwargs
) -> pd.DataFrame:
    source_df = pd.read_csv(io.BytesIO(bytes_data), encoding="ISO-8859-1")
    source_df["year"] = pd.to_datetime(source_df["year"], format="%Y")
    return source_df


async def unicef_waste_management_transform_preprocessing(
    bytes_data: bytes, **kwargs
) -> pd.DataFrame:
    def parse_year(x):
        if len(str(x)) > 2:
            return datetime.strptime(str(x), "%Y")
        else:
            return datetime.strptime(str(x), "%y")

    source_df = pd.read_excel(
        io.BytesIO(bytes_data), sheet_name="Waste Management", header=1
    )
    source_df["Year"] = source_df["Year"].apply(parse_year)
    source_df.replace("-", np.nan, inplace=True)
    source_df.replace(">99", 100, inplace=True)
    source_df.replace("<1", 0, inplace=True)
    source_df.replace("NA", np.nan, inplace=True)
    return source_df


async def ihme_pollution_transform_preprocessing(
    bytes_data: bytes, **kwargs
) -> pd.DataFrame:
    source_df = pd.read_csv(io.BytesIO(bytes_data), encoding="ISO-8859-1")
    source_df["year_id"] = pd.to_datetime(source_df["year_id"], format="%Y")
    source_df.replace("Côte d'Ivoire", "Ivory Coast", inplace=True)
    return source_df


async def un_stats_transform_preprocessing(bytes_data: bytes, **kwargs) -> pd.DataFrame:
    source_df = pd.read_csv(io.BytesIO(bytes_data))
    source_df["Time_Detail"] = (
        source_df["Time_Detail"].astype(str).str.replace(".0", "")
    )
    source_df["Time_Detail"] = source_df["Time_Detail"].apply(lambda x: x.strip())
    source_df["Time_Detail"] = source_df["Time_Detail"].replace("", np.nan)
    source_df = source_df.dropna(subset=["Time_Detail"])
    source_df["Time_Detail"] = pd.to_datetime(source_df["Time_Detail"], format="%Y")
    return source_df


async def unsd_sdg_api_transform_preprocessing(
    bytes_data: bytes, **kwargs
) -> pd.DataFrame:
    source_df = pd.read_csv(io.BytesIO(bytes_data), encoding="ISO-8859-1")
    source_df["Time_Detail"] = pd.to_datetime(source_df["Time_Detail"], format="%Y")
    source_df = source_df[source_df["[Sex]"] == kwargs.get("filter_sex_column")]
    # print(source_df[["GeoAreaName", "Time_Detail", "Value"]])
    # exit()
    return source_df


async def high_tech_transform_preprocessing(bytes_data: bytes, **kwargs):
    source_df = pd.read_csv(io.BytesIO(bytes_data))
    source_df["Time_Detail"] = pd.to_datetime(source_df["Time_Detail"], format="%Y")
    return source_df


async def owid_mental_health_transform_preprocessing(
    bytes_data: bytes, **kwargs
) -> pd.DataFrame:
    source_df = pd.read_csv(io.BytesIO(bytes_data))
    source_df["Year"] = pd.to_datetime(source_df["Year"], format="%Y")
    return source_df


async def wbank_hiv_transform_preprocessing(
    bytes_data: bytes, **kwargs
) -> pd.DataFrame:
    source_df = pd.read_csv(io.BytesIO(bytes_data), skiprows=4)
    return source_df


async def lgbtq_equality_transform_preprocessing(
    bytes_data: bytes, **kwargs
) -> pd.DataFrame:
    source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="Sheet1")
    source_df["Year"] = source_df["Year"].apply(
        lambda x: datetime.strptime(str(x), "%Y")
    )
    source_df["Country"] = source_df["Country"].apply(
        lambda x: " ".join(x.rsplit(" ")[1:])
    )
    return source_df


async def gho_api_who_transform_preprocessing(
    bytes_data: bytes, **kwargs
) -> pd.DataFrame:
    data = json.loads(bytes_data.decode("utf-8"))
    source_df = pd.DataFrame(pd.json_normalize(data["value"]))
    source_df["TimeDim"] = pd.to_datetime(source_df["TimeDim"], format="%Y")
    return source_df


async def undp_climate_change_transform_preprocessing(
    bytes_data: bytes, **kwargs
) -> pd.DataFrame:
    source_df = pd.read_csv(io.BytesIO(bytes_data))
    rename_columns_mapping = {
        "years_2020_2039": "2030",
        "years_2040_2059": "2040",
        "years_2080_2099": "2090",
    }
    source_df.rename(columns=rename_columns_mapping, inplace=True)
    return source_df


async def er_mrn_mpa_transform_preprocessing(
    bytes_data: bytes, **kwargs
) -> pd.DataFrame:
    source_df = pd.read_csv(io.BytesIO(bytes_data), encoding="ISO-8859-1")
    source_df = source_df[source_df["Value"] != "N"]
    source_df.dropna(subset=["Value"], inplace=True)
    source_df["Time_Detail"] = pd.to_datetime(source_df["Time_Detail"], format="%Y")
    source_df.replace("Türkiye", "Turkey", inplace=True)
    return source_df


async def inform_cc_transform_preprocessing(
    bytes_data: bytes, **kwargs
) -> pd.DataFrame:
    source_df = pd.read_excel(
        io.BytesIO(bytes_data),
        usecols=[
            "Unnamed: 0",
            "Unnamed: 15",
            "Unnamed: 18",
            "Unnamed: 21",
            "Unnamed: 24",
            "Unnamed: 26",
        ],
    )
    new_cols = ["Country", "Column P", "Column S", "Column V", "Column Y", "Column AA"]
    source_df.columns = new_cols
    source_df = source_df.iloc[4:]
    source_df.reset_index(drop=True, inplace=True)
    return source_df


async def mpi_transform_preprocessing(bytes_data: bytes, **kwargs) -> pd.DataFrame:
    source_df = pd.read_excel(
        io.BytesIO(bytes_data),
        sheet_name=kwargs.get("sheet_name"),
        header=4,
        skiprows=1,
    )
    country_column = source_df.columns[0]
    year_column = source_df.columns[2]
    value_column = source_df.columns[4]
    headcount_ratio = source_df.columns[6]
    intensity_of_deprivation = source_df.columns[10]
    source_df = source_df[
        [
            country_column,
            year_column,
            value_column,
            headcount_ratio,
            intensity_of_deprivation,
        ]
    ]
    source_df = source_df.rename(
        columns={
            country_column: "country",
            year_column: "year",
            value_column: "value",
            headcount_ratio: "headcount_ratio",
            intensity_of_deprivation: "intensity_of_deprivation",
        }
    )

    source_df["value"] = source_df["value"].apply(lambda x: round(x, 3))
    source_df["headcount_ratio"] = source_df["headcount_ratio"].apply(
        lambda x: round(x, 1)
    )
    source_df["intensity_of_deprivation"] = source_df["intensity_of_deprivation"].apply(
        lambda x: round(x, 1)
    )
    source_df["year"] = source_df["year"].apply(
        lambda x: str(x).split("/")[1] if "/" in str(x) else str(x)
    )
    source_df["year"] = source_df["year"].apply(lambda x: str(x).split(" ")[0])
    source_df.dropna(subset=["year"], inplace=True)

    pd.set_option("display.max_rows", None)
    source_df = source_df[source_df["year"] != "nan"]
    source_df["year"] = source_df["year"].apply(
        lambda x: datetime.strptime(str(x), "%Y")
    )
    return source_df


async def ihme_static_tansform_preprocessing(
    bytes_data: bytes, **kwargs
) -> pd.DataFrame:
    source_df = pd.read_csv(io.BytesIO(bytes_data), encoding="ISO-8859-1")
    source_df["year"] = pd.to_datetime(source_df["year"], format="%Y")
    return source_df


if __name__ == "__main__":
    pass
