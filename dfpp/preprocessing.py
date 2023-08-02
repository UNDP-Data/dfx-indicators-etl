import asyncio
import io
import os
from datetime import datetime
import re
import pandas as pd
import numpy as np

from dfpp.constants import STANDARD_COUNTRY_COLUMN, STANDARD_KEY_COLUMN
from dfpp.storage import StorageManager
from dfpp.utils import change_iso3_to_system_region_iso3, fix_iso_country_codes, add_country_code, add_region_code
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


async def acctoi_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the ACCTOI transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the ACCTOI transform.

    """

    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'

    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel data into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="INFRASTRUCTURE II EN")

        # Select the relevant rows from the DataFrame
        source_df = source_df.iloc[7:124]

        # Replace special characters with NaN values
        source_df = source_df.replace(['…', '-', 'x[16]'], [np.nan, np.nan, np.nan])

        # Rename the column "Unnamed: 0" to "Country"
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)

        return source_df
    except Exception as e:
        logger.error(f"Error in acctoi_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def bti_project_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the BTI project transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the BTI project transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel data into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data))

        # Select the relevant rows from the DataFrame
        source_df = source_df.iloc[6:124]

        # Replace special characters with NaN values
        source_df = source_df.replace(['…', '-', 'x[16]'], [np.nan, np.nan, np.nan])

        country_col_name = 'Regions:\n1 | East-Central and Southeast Europe\n2 | Latin America and the Caribbean\n3 | West and Central Africa\n4 | Middle East and North Africa\n5 | Southern and Eastern Africa\n6 | Post-Soviet Eurasia\n7 | Asia and Oceania'
        # Rename the column with long name to "Country"
        source_df.rename(columns={country_col_name: "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        return source_df
    except Exception as e:
        logger.error(
            f"Error in bti_project_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def cpi_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the CPI transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the CPI transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel data into a DataFrame
        source_df = pd.read_json(io.BytesIO(bytes_data))

        # Rename the columns
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code",
                                  kwargs.get("year_column"): "Year"},
                         inplace=True)

        # Convert the "Year" column to datetime format
        source_df["year"] = source_df["year"].apply(lambda x: datetime.strptime(str(x), '%Y'))
        return source_df
    except Exception as e:
        logger.error(f"Error in cpi_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def cpia_rlpr_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the CPIA RLPR transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the CPIA RLPR transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV data into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))

        # Replace ".." with NaN values
        source_df.replace("..", np.nan, inplace=True)

        # Rename the columns
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)

        return source_df
    except Exception as e:
        logger.error(
            f"Error in cpia_rlpr_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def cpia_spca_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the CPIA SPCA transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the CPIA SPCA transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV data into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))

        # Rename the columns
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)

        return source_df
    except Exception as e:
        logger.error(
            f"Error in cpia_spca_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def cpia_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the CPIA transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the CPIA transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")

        # Read the CSV data into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data), header=2)

        # Replace ".." with NaN values
        source_df.replace("..", np.nan, inplace=True)

        # Rename the columns
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)

        return source_df
    except Exception as e:
        logger.error(f"Error in cpia_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def cw_ndc_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the CW NDC transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the CW NDC transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        source_df = pd.read_csv(io.BytesIO(bytes_data))

        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)

        def reorganize_number_ranges(text):
            # Reorganize number ranges in the text to have consistent formatting
            pattern = re.compile(r'(-?\d+)-(-?\d+)')
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
            return re.sub(r'(?<!\d)\.(\d+)', r'0.\1', text)

        def extract_and_change(text):
            # Extract the number and apply changes to the text based on the unit
            numbers = re.findall(r"-?\d+\.?\d*", text)
            if numbers:
                number = float(numbers[0])
                if ("ktGgCO" in text) or ("GgCO" in text):
                    number /= 10 ** 3
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
            source_df["M_TarA3"] = source_df["M_TarA3"].str.replace(key, value, regex=False)
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
        source_df["ndc_date"] = pd.to_datetime(source_df["ndc_date"], format='%m/%d/%Y')
        source_df["M_TarYr"] = pd.to_datetime(source_df["M_TarYr"], format='%Y')
        # filter latest values
        source_df.sort_values("ndc_date", inplace=True, ascending=True)
        source_df.drop_duplicates(subset=["ISO"], keep="last", inplace=True)
        return source_df
    except Exception as e:
        logger.error(f"Error in eb_ndc_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def cw_t2_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the CW T2 transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the CW T2 transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV data into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))

        # rename the country and key columns based on the kwargs
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Return the DataFrame without any further preprocessing
        return source_df
    except Exception as e:
        logger.error(f"Error in cw_t2_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def eb_wbdb_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the EB WBDB transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the EB WBDB transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    global df
    indicator_name = "Ease of doing business score"
    assert indicator_name is not None, f'indicator_id is not set in kwargs'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV data into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="DB Data (As of DB20)", header=3)
        year_col_mapping = {
            2016: '(DB17-20 methodology)',
            2014: '(DB15 methodology)',
            2010: '(DB10-14 methodology)'
        }
        group_df = source_df.groupby('DB Year')

        group_column_mapping = {}
        for year in group_df.groups.keys():
            for db_year in year_col_mapping.keys():
                if int(year) >= db_year:
                    indicator_suffix = year_col_mapping[db_year]
                    if " ".join([indicator_name, indicator_suffix]) not in group_column_mapping.keys():
                        group_column_mapping[" ".join([indicator_name, indicator_suffix])] = []
                    group_column_mapping[" ".join([indicator_name, indicator_suffix])].append(year)
                    break

            for key in group_column_mapping.keys():
                df = pd.concat(group_df.get_group(year) for year in group_column_mapping[key])
                df['DB Year'] = df['DB Year'].apply(lambda x: datetime.strptime(str(int(float(x))), '%Y'))
        # Return the preprocessed DataFrame
        pd.set_option('display.max_columns', None)
        df = df[['Country code', 'Economy', 'Ease of doing business score (DB17-20 methodology)',
                 'Ease of doing business score (DB15 methodology)',
                 'Ease of doing business score (DB10-14 methodology)']]
        df.rename(columns={
            'Ease of doing business score (DB17-20 methodology)': '2016',
            'Ease of doing business score (DB15 methodology)': '2014',
            'Ease of doing business score (DB10-14 methodology)': '2010'
        }, inplace=True)
        return df
    except Exception as e:
        logger.error(f"Error in eb_wbdb_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def fao_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the FAO transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the FAO transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel data into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="Data")
        # source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
        #                  inplace=True)
        # Extract the year from the "Time_Detail" column
        source_df["Time_Detail"] = source_df["Time_Detail"].apply(lambda x: str(x).rsplit("-")[0])
        source_df["Time_Detail"] = source_df["Time_Detail"].apply(lambda x: datetime.strptime(str(x), '%Y'))
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(f"Error in fao_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def ff_dc_ce_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the FF-DC-CE transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the FF-DC-CE transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel data into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="Data")
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Convert the "TimePeriod" column to datetime format
        source_df["TimePeriod"] = pd.to_datetime(source_df["TimePeriod"], format='%Y')

        # Filter the DataFrame to include only rows with "Type of renewable technology" as "All renewables"
        source_df = source_df[source_df["Type of renewable technology"] == "All renewables"]
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(f"Error in ff_dc_ce_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def ghg_ndc_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the GHG-NDC transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the GHG-NDC transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV data into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Filter the DataFrame to include only rows with "Sector" as "Total including LUCF" and "Gas" as "All GHG"
        source_df = source_df[(source_df["Sector"] == "Total including LUCF") & (source_df["Gas"] == "All GHG")]

        # Call the change_iso3_to_system_region_iso3 function to perform additional preprocessing on the DataFrame
        source_df = await change_iso3_to_system_region_iso3(source_df, "Country")
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(f"Error in ghg_ndc_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def gii_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the GII transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the GII transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV data into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Return the DataFrame without any preprocessing
        return source_df
    except Exception as e:
        logger.error(f"Error in gii_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def global_data_fsi_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the Global Data FSI transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the Global Data FSI transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV data into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Return the DataFrame without any preprocessing
        return source_df
    except Exception as e:
        logger.error(
            f"Error in global_data_fsi_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def global_findex_database_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the Global Findex Database transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the Global Findex Database transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel data into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="Data", header=0, skiprows=0)

        # Rename columns
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)

        source_df.rename(columns=lambda x: x.replace("%", "percentage").replace("+", "_plus"), inplace=True)
        # Convert "Year" column to datetime format
        source_df["Year"] = pd.to_datetime(source_df["Year"], format='%Y')
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in global_findex_database_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def global_pi_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the Global Political Institutions transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the Global Political Institutions transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV data into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Return the DataFrame without any additional preprocessing
        return source_df
    except Exception as e:
        logger.error(
            f"Error in global_pi_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def eil_pe_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the EIL PE transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the EIL PE transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="7.3 Official indicator", skiprows=2)
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Drop rows with missing values ("..")
        source_df.drop(source_df[source_df['Value'] == ".."].index, inplace=True)

        # Reset the index
        source_df.reset_index(inplace=True)

        # Convert the "TimePeriod" column to datetime format
        source_df["TimePeriod"] = pd.to_datetime(source_df["TimePeriod"], format='%Y')
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(f"Error in eil_pe_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def ec_edu_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the EC EDU transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the EC EDU transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame, skipping rows and selecting columns
        source_df = pd.read_excel(io.BytesIO(bytes_data), skiprows=10, nrows=202, usecols=[i for i in range(0, 12)])
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Define the new column names
        new_column_names = ['Country', 'Total', 'Footnote_total', 'Male', 'fn_male', 'Female', 'fn_female', 'Poorest',
                            'fn_poorest', 'Richest', 'fn_richest', 'Source']

        # Rename the columns
        source_df.columns = new_column_names
        # Drop rows with missing values in the "Source" column
        source_df.dropna(subset=['Source'], inplace=True)

        # Replace "-" with NaN values
        source_df.replace("-", np.nan, inplace=True)

        # Drop rows with "Source" value of "MICS2"
        source_df.drop(source_df[source_df['Source'] == "MICS2"].index, inplace=True)

        # Reset the index
        source_df.reset_index(inplace=True)

        # Extract the year from the "Source" column and create a new "Year" column
        year = []
        for i in source_df['Source']:
            yr = re.findall(r'\d{4}', i)
            year.append(yr[0])
        source_df['Year'] = year
        source_df["Year"] = pd.to_datetime(source_df["Year"], format='%Y')
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(f"Error in ec_edu_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def hdr_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the HDR transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the HDR transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        source_df = pd.read_csv(io.BytesIO(bytes_data))
        source_df = await change_iso3_to_system_region_iso3(source_df, "Alpha-3 code")
        source_df.reset_index(inplace=True)
        await fix_iso_country_codes(df=source_df, col="Alpha-3 code", source_id="HDR")
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        return source_df
    except Exception as e:
        logger.error(f"Error in hdr_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def heritage_id_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the Heritage Index of Economic Freedom transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the Heritage Index of Economic Freedom transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data))

        # Rename columns
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)

        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in heritage_id_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def ilo_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        source_df = pd.read_excel(io.BytesIO(bytes_data), header=5)
        source_df["Time"] = source_df["Time"].apply(lambda x: datetime.strptime(str(x), '%Y'))
        return source_df
    except Exception as e:
        logger.error(f"Error in ilo_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def ilo_ee_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the ILO Employment and Earnings transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the ILO Employment and Earnings transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV file into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))

        # Replace "none" values with NaN
        source_df = source_df.replace("none", np.nan)

        # Convert the "Year" column to datetime format
        source_df["Year"] = source_df["Year"].apply(lambda x: datetime.strptime(str(x), '%Y'))
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(f"Error in ilo_ee_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def ilo_lfs_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the ILO Labor Force Survey (LFS) transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the ILO LFS transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame, skipping the first 5 rows
        source_df = pd.read_excel(io.BytesIO(bytes_data), header=5)

        # Convert the "Time" column to datetime format
        source_df["Time"] = source_df["Time"].apply(lambda x: datetime.strptime(str(x), '%Y'))
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(f"Error in ilo_lfs_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def ilo_nifl_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the ILO National Income from Labor (NIFL) transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the ILO NIFL transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame, skipping the first 5 rows
        source_df = pd.read_excel(io.BytesIO(bytes_data), header=5)

        # Convert the "Time" column to datetime format
        source_df["Time"] = source_df["Time"].apply(lambda x: datetime.strptime(str(x), '%Y'))
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(f"Error in ilo_nifl_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def imf_weo_baseline_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the IMF World Economic Outlook (WEO) baseline transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the IMF WEO baseline transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data))

        # Replace "--" values with NaN
        source_df.replace("--", np.nan, inplace=True)

        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in imf_weo_baseline_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def imf_weo_gdp_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    :param bytes_data:
    :param kwargs:
    :return:
    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data))
        source_df.replace("--", np.nan, inplace=True)
        return source_df
    except Exception as e:
        logger.error(
            f"Error in imf_weo_gdp_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def imf_weo_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the IMF World Economic Outlook (WEO) transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the IMF WEO transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'

    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data))
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Replace "--" values with NaN
        source_df.replace("--", np.nan, inplace=True)

        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(f"Error in imf_weo_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def iec_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the International Energy Council (IEC) transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the IEC transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data), header=1)
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Drop columns with all NaN values
        source_df.dropna(axis=1, how='all', inplace=True)

        # Assign new column names
        new_column_names = ['Country', 'Energy Type', 'On/Off grid', 'Year', 'Value']
        source_df.columns = new_column_names

        # Forward fill NaN values in the DataFrame
        source_df = source_df.fillna(method="ffill")

        # Drop rows where the value is ".."
        source_df.drop(source_df[source_df['Value'] == ".."].index, inplace=True)

        # Convert the "Year" column to datetime format
        source_df["Year"] = pd.to_datetime(source_df["Year"], format='%Y')

        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(f"Error in iec_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def imsmy_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the International Monetary Statistics (IMSMY) transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the IMSMY transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="Table 1", header=10)
        # Replace ".." with NaN values
        source_df.replace("..", np.nan, inplace=True)

        # Rename the "Region, development group, country or area" column to "Country"

        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)

        # Remove spaces and asterisks from the "Country" column values
        source_df["Country"] = source_df["Country"].apply(lambda x: x.replace(" ", "").replace("*", ""))

        # Convert the "Year" column to datetime format
        source_df["Year"] = source_df["Year"].apply(lambda x: datetime.strptime(str(x), '%Y'))

        # Add country codes to the DataFrame
        source_df = await add_country_code(source_df, "Country")
        # Add region codes to the DataFrame
        source_df = await add_region_code(source_df, "Country")

        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(f"Error in imsmy_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def inequality_hdi_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the Inequality and Human Development Index (HDI) transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the Inequality and HDI transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="Table 3", header=4)
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Select the desired rows (from row 1 to 200)
        source_df = source_df.iloc[1:200]

        # Replace ".." with NaN values
        source_df = source_df.replace("..", np.nan)

        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in inequality_hdi_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def isabo_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the ISABO transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the JSON file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the ISABO transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the JSON file into a DataFrame
        source_df = pd.read_json(io.BytesIO(bytes_data))

        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)

        # Flatten the "table1" column into separate columns
        source_df = pd.json_normalize(source_df["table1"])

        # Convert the "year" column to float
        source_df["year"] = source_df["year"].astype(float)

        # Drop rows with missing values in the "year" column
        source_df.dropna(inplace=True, subset=["year"])

        # Replace "NaN" strings with NaN values
        source_df.replace("NaN", np.nan, inplace=True)

        # Convert the "year" column to datetime format
        source_df["year"] = source_df["year"].apply(lambda x: datetime.strptime(str(round(x)), '%Y'))

        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(f"Error in isabo_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def itu_ict_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the ITU ICT transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the ITU ICT transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="i99H")
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(f"Error in itu_ict_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def mdp_bpl_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the MDP BPL transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the MDP BPL transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data))
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Remove the first row from the DataFrame
        source_df = source_df.iloc[1:]
        # Extract the country name from the "countryname" column
        source_df["Country"] = source_df["Country"].apply(lambda x: " ".join(x.rsplit(" ")[1:]))
        # Extract the year from the "period" column
        source_df["period"] = source_df["period"].apply(lambda x: datetime.strptime(str(x).rsplit(" ")[-1], '%Y'))
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(f"Error in mdp_bpl_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


# async def mdp_indicators_taf_transform_preprocessing(bytes_data: bytes= None):
#     source_df = pd.read_json(io.BytesIO(bytes_data))
#     source_df

async def mdp_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the MDP transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the JSON file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the MDP transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")

        async def mdp_metadata():
            async with StorageManager() as storage:
                country_df_bytes = await storage.download(
                    blob_name=os.path.join(os.environ.get("ROOT_FOLDER"), 'config', 'utilities', 'MDP_META.json'))
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
        source_df["t"] = source_df["t"].apply(lambda x: datetime.strptime(str(x), '%Y'))
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(f"Error in mdp_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def natural_capital_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the natural capital transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the natural capital transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data))
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Calculate the total natural capital by summing the renewable and nonrenewable natural capital
        source_df["Total"] = source_df["Renewable natural capital"] + source_df["Nonrenewable natural capital"]

        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in natural_capital_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def nature_co2_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the nature CO2 transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the nature CO2 transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data), header=1)
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Convert a column to percentages
        source_df["percent.6"] = source_df["percent.6"].apply(lambda x: x * 100)

        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in nature_co2_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def nd_climate_readiness_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the climate readiness transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the climate readiness transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV file into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Define the non-value columns
        non_values_columns = ["ISO3", "Name"]

        # Add rank columns for each non-value column
        for column_name in source_df.columns:
            if column_name in non_values_columns:
                continue
            else:
                source_df[column_name + "_rank"] = source_df[column_name].rank(method="dense", ascending=False)

        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in nd_climate_readiness_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def oecd_raw_mat_consumption_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the OECD raw material consumption transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the OECD raw material consumption transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV file into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Drop rows with missing values in the 'Value' column
        source_df = source_df.dropna(subset=['Value'])

        # Convert the 'TIME' column to datetime format
        source_df['TIME'] = source_df['TIME'].apply(lambda x: datetime.strptime(str(x), '%Y'))

        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in oecd_raw_mat_consumption_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def owid_energy_data_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the OWID energy data transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the OWID energy data transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV file into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))

        # Drop rows with missing values in the 'year' column
        source_df.dropna(subset=["year"], inplace=True)

        # Convert the 'year' column to datetime format
        source_df["year"] = source_df["year"].apply(lambda x: datetime.strptime(str(round(float(x))), '%Y'))

        # Add region codes to the DataFrame
        source_df = await add_region_code(source_df, "country", "iso_code")
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in owid_energy_data_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def owid_export_transform(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the OWID export transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the OWID export transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV file into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))

        # Drop rows with missing values in the 'Year' column
        source_df.dropna(subset=["Year"], inplace=True)

        # Convert the 'Year' column to datetime format
        source_df["Year"] = source_df["Year"].apply(lambda x: datetime.strptime(str(round(float(x))), '%Y'))

        # Add region codes to the DataFrame
        source_df = await add_region_code(source_df, "Entity", "Code")
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(f"Error in owid_export_transform: {e}")
        raise e


async def owid_oz_consumption_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the OWID ozone consumption transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the OWID ozone consumption transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
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
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in owid_oz_consumption_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def owid_t3_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the OWID T3 transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the OWID T3 transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
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
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(f"Error in owid_t3_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def owid_trade_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the OWID trade transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the OWID trade transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")

        # Read the CSV file into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))
        country_col_name = 'Entity'
        iso_col_name = 'Code'

        # Drop rows with missing values in the Year column
        source_df.dropna(subset=["Year"], inplace=True)
        source_df["Year"] = source_df["Year"].apply(lambda x: datetime.strptime(str(round(float(x))), '%Y'))

        # Add region code to the DataFrame using the add_region_code function
        source_df = await add_region_code(source_df=source_df, region_name_col=country_col_name,
                                          region_key_col=iso_col_name)
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in owid_trade_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def oxcgrt_rl_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the OxCGRT RL transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the OxCGRT RL transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV file into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))

        # Convert the "Date" column to datetime format
        source_df["Date"] = source_df["Date"].apply(lambda x: datetime.strptime(str(x), '%Y%m%d'))

        # Update the "StringencyIndex_Average" column with values from "StringencyIndex_Average_ForDisplay"
        source_df["StringencyIndex_Average_ForDisplay"].update(source_df["StringencyIndex_Average"])
        source_df["StringencyIndex_Average"] = source_df["StringencyIndex_Average_ForDisplay"]
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in oxcgrt_rl_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def pts_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the PTS transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the PTS transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV file into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))

        # Convert the "Year" column to datetime format
        source_df["Year"] = source_df["Year"].apply(lambda x: datetime.strptime(str(x), '%Y'))

        # Replace "NA" values with NaN
        source_df.replace("NA", np.nan, inplace=True)
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(f"Error in pts_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def sdg_mr_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the SDG MR transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the SDG MR transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="Table format")
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(f"Error in sdg_mr_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def sdg_rap_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the SDG RAP transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the SDG RAP transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data), header=5)

        # Convert the "Time" column to datetime format
        source_df["Time"] = source_df["Time"].apply(lambda x: datetime.strptime(str(x), '%Y'))
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(f"Error in sdg_mr_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def sipri_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the SIPRI transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the SIPRI transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="Share of Govt. spending", header=7)

        # Replace "..." with NaN values
        source_df.replace(["...", "xxx"], [np.nan, np.nan], inplace=True)
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(f"Error in sipri_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def undp_gii_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the UNDP GII transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the UNDP GII transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data), header=5)

        # Select the relevant rows
        source_df = source_df.iloc[2:202]

        # Replace ".." with NaN values
        source_df.replace("..", np.nan, inplace=True)
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(f"Error in undp_gii_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def undp_hdi_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the UNDP HDI transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the UNDP HDI transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV file into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(f"Error in undp_hdi_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def undp_mpi_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the UNDP MPI transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the UNDP MPI transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame (Table 2)
        source_df = pd.read_excel(io.BytesIO(bytes_data), header=4, sheet_name="Table 2")

        # Filter rows and columns
        source_df = source_df.iloc[:188]

        # Read the Excel file into a DataFrame (Table 1 for region data)
        region_source_df = pd.read_excel(io.BytesIO(bytes_data), header=4, sheet_name="Table 1")

        # Extract the relevant region data
        start = region_source_df[region_source_df['Country'] == 'Developing countries'].index[0]
        end = region_source_df[region_source_df['Country'] == 'Notes'].index[0]
        region_source_df = region_source_df.iloc[start:end - 2]
        region_source_df_sub = region_source_df[['Country', 'Value']]
        region_source_df_sub.rename(columns={'Country': 'Unnamed: 0'}, inplace=True)

        # Extract the year from column name and append region data to source_df
        region_year = region_source_df.columns[2].split('-')[-1]
        region_source_df_sub['Unnamed: 2'] = region_year
        source_df = pd.concat([source_df, region_source_df_sub], axis=0)

        # Preprocess columns
        source_df["Unnamed: 2"] = source_df["Unnamed: 2"].apply(lambda x: x.rsplit(" ")[0].rsplit("/")[-1])
        source_df["Unnamed: 2"] = source_df["Unnamed: 2"].apply(lambda x: datetime.strptime(str(x), '%Y'))
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(f"Error in undp_mpi_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def unescwa_fr_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the UNESCWA FR transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the UNESCWA FR transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="Sheet1")

        # Multiply the 'Government fiscal support (Bn USD) 2020 & 2021' column by (10 ** 9)
        source_df['Government fiscal support (Bn USD) 2020 & 2021'] = source_df[
            'Government fiscal support (Bn USD) 2020 & 2021'].apply(lambda x: x * (10 ** 9))
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in unescwa_fr_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def unicef_dev_ontrk_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the UNICEF Development On-Track transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the UNICEF Development On-Track transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV file into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))

        # Convert the 'TIME_PERIOD' column to datetime format
        source_df["TIME_PERIOD"] = source_df["TIME_PERIOD"].apply(lambda x: datetime.strptime(str(x), '%Y'))

        # Replace 'No data' values with NaN
        source_df.replace(['No data'], [np.nan], inplace=True)

        # Perform additional preprocessing if required, e.g., changing ISO3 codes to system region ISO3 codes
        source_df = await change_iso3_to_system_region_iso3(source_df, "REF_AREA")
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in unicef_dev_ontrk_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def untp_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the United Nations Population Estimates transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the United Nations Population Estimates transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="Estimates", header=16)

        # Drop rows with missing values in the 'Year' column
        source_df.dropna(subset=["Year"], inplace=True)

        # Convert the values in the 'Year' column to datetime format
        source_df["Year"] = source_df["Year"].apply(lambda x: datetime.strptime(str(round(float(x))), '%Y'))

        # Convert population values from thousands to actual values
        source_df["Total Population, as of 1 January (thousands)"] = source_df[
            "Total Population, as of 1 January (thousands)"].apply(lambda x: x * 1000)

        # Perform additional preprocessing if required, e.g., adding region codes
        source_df = await add_region_code(source_df, "Region, subregion, country or area *", "ISO3 Alpha-code")
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(f"Error in untp_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def vdem_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the Varieties of Democracy transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the Varieties of Democracy transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV file into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data))

        # Convert the values in the 'year' column to datetime format
        source_df["year"] = source_df["year"].apply(lambda x: datetime.strptime(str(x), '%Y'))
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(f"Error in vdem_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def time_udc_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the Time UDC transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the CSV file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the Time UDC transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the CSV file into a DataFrame
        source_df = pd.read_csv(io.BytesIO(bytes_data), encoding='ISO-8859-1', skiprows=1)

        # Convert the values in the 'Period' column to datetime format
        source_df[" Period"] = pd.to_datetime(source_df[" Period"], format='%Y')
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(f"Error in time_udc_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def wbank_access_elec_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the World Bank Electricity Access transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the World Bank Electricity Access transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="Data", header=0)

        # Rename the columns to match the desired format
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in wbank_access_elec_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def wbank_info_eco_transform_preprocessing(bytes_data: bytes = None, sheet_name=None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the World Bank Economic Information transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.
        sheet_name (str or int, optional): The name or index of the sheet to read. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the World Bank Economic Information transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name=sheet_name, header=0)
        # strip the column names in pandas dataframe
        source_df.rename(columns=lambda x: str(x).strip(), inplace=True)
        # Rename the columns to match the desired format
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)

        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in wbank_info_eco_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def wbank_info_transform_preprocessing(bytes_data: bytes = None, sheet_name=None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the World Bank Information transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.
        sheet_name (str or int, optional): The name or index of the sheet to read. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the World Bank Information transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    assert sheet_name is not None, "Sheet name cannot be None"
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name=sheet_name, header=13)
        # Create a dictionary to hold the column renaming information
        column_rename = {}

        # Iterate over the columns and generate new column names
        for column in source_df.columns:
            column_rename[column] = str(column) + "_" + source_df.iloc[0][column]

        # Rename the columns using the column_rename dictionary
        source_df.rename(columns=column_rename, inplace=True)

        # Remove the first row (header row) from the DataFrame
        source_df = source_df.iloc[1:]
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in wbank_info_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def wbank_poverty_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the World Bank Poverty transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the World Bank Poverty transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data), header=0, skiprows=3)

        # Select the desired columns from the DataFrame
        source_df = source_df[['code', 'economy', 'mdpoor_i1', 'year']]

        # Drop rows with missing values
        source_df.dropna(inplace=True)

        # Create a dictionary to hold the column renaming information
        column_rename = {}

        # Rename the columns using the column_rename dictionary
        source_df.rename(columns=column_rename, inplace=True)

        # Convert the 'year' column datatype to a datetime object
        source_df["year"] = pd.to_datetime(source_df["year"], format='%Y')
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in wbank_poverty_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def wbank_energy_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the World Bank Energy transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the World Bank Energy transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
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
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in wbank_energy_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def wbank_rai_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the World Bank RAI transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the World Bank RAI transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="RAI Crosstab", header=2)

        # Rename the columns with a prefix "year_" for integer columns
        source_df.columns = [f"year_{col}" if isinstance(col, int) else col for col in source_df.columns]

        # Drop rows with all NaN values
        source_df = source_df.dropna(how='all')
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(
            f"Error in wbank_rai_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def wbank_t1_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the World Bank Table 1 transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the Excel file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the World Bank Table 1 transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the Excel file into a DataFrame
        source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="Data", header=3)
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(f"Error in wbank_t1_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def who_pre_edu_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the WHO Pre-Education Statistics transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the JSON file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the WHO Pre-Education Statistics transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the JSON data into a DataFrame
        source_df = pd.read_json(io.BytesIO(bytes_data))

        # Extract the "value" column
        source_df = source_df["value"]

        # Normalize the JSON data into a DataFrame
        source_df = pd.json_normalize(source_df)

        # Convert the "TimeDim" column to datetime format
        source_df["TimeDim"] = source_df["TimeDim"].apply(lambda x: datetime.strptime(str(x), '%Y'))

        # Pivot the DataFrame to reshape the data
        pivot_df = source_df.pivot_table(
            values='NumericValue',
            index=['SpatialDim', 'TimeDim'],
            columns='Dim1'
        ).reset_index()

        # Replace 'No data' values with NaN
        pivot_df.replace(['No data'], [np.nan], inplace=True)

        # Map ISO3 codes to system region ISO3 codes
        pivot_df = await change_iso3_to_system_region_iso3(pivot_df, "SpatialDim")
        pivot_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                        inplace=True)
        # Return the preprocessed DataFrame
        return pivot_df
    except Exception as e:
        logger.error(
            f"Error in who_pre_edu_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def who_rl_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    """
    Preprocesses the data for the WHO Right to Health transform.

    Args:
        bytes_data (bytes, optional): The bytes data of the JSON file. Defaults to None.

    Returns:
        pandas.DataFrame: The preprocessed DataFrame for the WHO Right to Health transform.

    """
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        logger.info(f"Running preprocessing for indicator {kwargs.get('indicator_id')}")
        # Read the JSON data into a DataFrame
        source_df = pd.read_json(io.BytesIO(bytes_data))
        # Extract the "value" column
        source_df = source_df["value"]

        # Normalize the JSON data into a DataFrame
        source_df = pd.json_normalize(source_df)

        # Replace 'No data' values with NaN
        source_df.replace(['No data'], [np.nan], inplace=True)

        # Convert the "TimeDim" column to datetime format
        source_df["TimeDim"] = source_df["TimeDim"].apply(lambda x: datetime.strptime(str(x), '%Y'))

        # Map ISO3 codes to system region ISO3 codes
        source_df = await change_iso3_to_system_region_iso3(source_df, "SpatialDim")
        source_df.rename(columns={kwargs.get("country_column"): "Country", kwargs.get("key_column"): "Alpha-3 code"},
                         inplace=True)
        # Return the preprocessed DataFrame
        return source_df
    except Exception as e:
        logger.error(f"Error in who_rl_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def wbdb_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        source_df = pd.read_excel(io.BytesIO(bytes_data))
        source_df["Economy"] = source_df["Economy"].apply(lambda x: x.rsplit("*")[0] if '*' in str(x) else x)
        source_df.dropna(subset=["Year"], inplace=True)
        source_df["Year"] = source_df["Year"].apply(lambda x: datetime.strptime(str(int(x)), '%Y'))
        return source_df
    except Exception as e:
        logger.error(f"Error in wbdb_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def ilo_sdg_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        source_df = pd.read_excel(io.BytesIO(bytes_data), header=5)
        source_df["Time"] = source_df["Time"].apply(lambda x: datetime.strptime(str(x), '%Y'))
        return source_df
    except Exception as e:
        logger.error(f"Error in ilo_sdg_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def ilo_spf_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        source_df = pd.read_excel(io.BytesIO(bytes_data), header=5)
        source_df["Time"] = source_df["Time"].apply(lambda x: datetime.strptime(str(x), '%Y'))
        return source_df
    except Exception as e:
        logger.error(f"Error in ilo_spf_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def imf_ifi_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="financial assistance", header=3,
                                  engine='openpyxl')
        source_df2 = pd.read_excel(io.BytesIO(bytes_data), sheet_name="debt-relief", engine='openpyxl')
        source_df2.rename(columns={"country": "Country", "source": "Type of Emergency Financing",
                                   "amount approved in SDR": "Amount Approved in SDR",
                                   "amount approved in USD": "Amount Approved in US$",
                                   "date of approval": "Date of Approval"}, inplace=True)
        source_df = pd.concat([source_df, source_df2], ignore_index=True)

        # source_df["Date of Approval"] = source_df["Date of Approval"].apply(lambda x: "20" + x.rsplit("-")[-1])
        source_df.dropna(subset=["Date of Approval"], inplace=True)
        # Convert the 'Date of Approval' column to datetime using datetime.strptime
        source_df['Date of Approval'] = source_df['Date of Approval'].apply(
            lambda x: datetime.strptime(str(x).rsplit(" ")[0], '%Y-%m-%d'))
        source_df["Amount Approved in US$"] = source_df["Amount Approved in US$"].apply(
            lambda x: float(x.rsplit("mill")[0].replace(",", "").replace("US$", "")) * (10 ** 6))
        return source_df
    except Exception as e:
        logger.error(f"Error in imf_ifi_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def who_global_rl_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        source_df = pd.read_csv(io.BytesIO(bytes_data))
        source_df["Date_reported"] = source_df["Date_reported"].apply(lambda x: datetime.strptime(str(x), '%Y-%m-%d'))
        return source_df
    except Exception as e:
        logger.error(
            f"Error in who_global_rl_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


async def sme_transform_preprocessing(bytes_data: bytes = None, **kwargs) -> pd.DataFrame:
    assert isinstance(bytes_data, bytes), f'bytes_data arg needs to be of type bytes'
    try:
        source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="Time Series", engine='openpyxl')
        source_df.replace(":", np.nan, inplace=True)
        source_df = source_df.iloc[1:]
        source_df.reset_index(inplace=True)
        source_df.rename(columns={"Country": STANDARD_COUNTRY_COLUMN, "Country Code": STANDARD_KEY_COLUMN},
                         inplace=True)
        source_df.rename(columns=lambda x: x.replace("\n", ""), inplace=True)
        return source_df
    except Exception as e:
        logger.error(f"Error in sme_transform_preprocessing: {e} while preprocessing {kwargs.get('indicator_id')}")
        raise e


if __name__ == "__main__":
    pass
