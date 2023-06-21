import pandas as pd

from dfpp.constants import STANDARD_KEY_COLUMN, STANDARD_COUNTRY_COLUMN
from dfpp.utils import get_year_columns, rename_indicator, invert_dictionary, add_country_code, add_region_code, \
    update_base_file


async def type1_transform(**kwargs):
    """
    Perform a type 1 transformation on the source DataFrame.

    Args:
        source_df (pd.DataFrame): Source DataFrame for transformation.
        indicator_id (str): Identifier for the indicator.
        base_filename (str): Base filename for saving the transformed data.
        country_column (str): Name of the country column.
        key_column (str): Name of the key column.
        column_prefix (str): Prefix for indicator columns.
        column_suffix (str): Suffix for indicator columns.
        column_substring (str): Substring for indicator columns.
        group_column (str): Name of the group column.
        group_name (str): Name of the group.
        aggregate (bool): Flag indicating whether to aggregate the data.
        keep (str): Value to keep during aggregation.

    Returns:
        Transformed data uploaded to a blob as a CSV file.
    """

    df = kwargs.get("source_df")
    indicator_id = kwargs.get('indicator_id', None)
    base_filename = kwargs.get('base_filename', None)
    country_column = kwargs.get('country_column', None)
    key_column = kwargs.get('key_column', None)
    column_prefix = kwargs.get('column_prefix', None)
    column_suffix = kwargs.get('column_suffix', None)
    column_substring = kwargs.get('column_substring', None)
    group_column = kwargs.get('group_column', None)
    group_name = kwargs.get('group_name', None)
    aggregate = kwargs.get('aggregate', False)
    keep = kwargs.get('keep', 'last')

    index_col = key_column if key_column else country_column

    if group_name and group_column:
        # Filter the DataFrame based on the group name
        df = df.groupby(group_column).get_group(group_name)
        df.dropna(inplace=True, axis=1, how="all")
    year_columns = await get_year_columns(df.columns, col_prefix=column_prefix, col_suffix=column_suffix,
                                          column_substring=column_substring)

    indicator_rename = {}
    for year in year_columns:
        indicator_rename[year_columns[year]] = await rename_indicator(indicator_id, year)

    inverted_dictionary = await invert_dictionary(indicator_rename)
    indicator_cols = list(inverted_dictionary.keys())

    df.rename(columns=indicator_rename, inplace=True)

    if not aggregate:
        # Drop duplicates based on the index column
        df.drop_duplicates(subset=[index_col], keep=keep, inplace=True)
    elif aggregate:
        # Aggregate the data based on the index column
        index_df = df.copy()
        group_df = index_df.groupby(index_col)
        df.set_index(index_col, inplace=True)

        for group in group_df.groups:
            temp_df = group_df.get_group(group)

            if len(temp_df) > 1:
                df.drop(group, inplace=True)
                temp_df.dropna(inplace=True, axis=1, how="all")
                temp_df = temp_df.sum()
                temp_df = temp_df.drop([index_col])
                df.at[group] = temp_df

        df.reset_index(inplace=True)

    if not key_column:
        # If key_column is not provided, keep only the country column
        df = df[[country_column] + indicator_cols]
        df = await add_country_code(df, country_name_column=country_column)
        df = await add_region_code(df, country_column)
    elif not country_column:
        # If country_column is not provided, keep only the key column and indicator columns
        df = df[[key_column] + indicator_cols]
    else:
        # If both key_column and country_column are provided, add region code and rearrange columns
        df = await add_region_code(df, country_column)
        df = df[[key_column, country_column] + indicator_cols]

    save_as = base_filename + ".csv"
    return await update_base_file(df=df, blob_name=save_as)


async def type2_transform(**kwargs):
    """
    Transforms a DataFrame based on the provided parameters.

    Args:
        source_df (pandas.DataFrame): The source DataFrame.
        indicator_id (str): The indicator ID.
        value_column (str): The name of the value column.
        base_filename (str): The base filename for saving the transformed DataFrame.
        country_column (str): The name of the country column.
        key_column (str): The name of the key column.
        year (str): The year.
        group_column (str): The name of the column for grouping.
        group_name (str): The name of the group.
        aggregate (bool): Whether to aggregate the DataFrame.
        keep (str): The strategy for keeping duplicate values.
        return_dataframe (bool): Whether to return the transformed DataFrame.
        country_code_aggregate (bool): Whether to aggregate by country code.
        region_column (str): The name of the region column.

    Returns:
        str: The name of the uploaded CSV file in the blob storage.
    """
    df = kwargs.get("source_df")
    indicator_id = kwargs.get('indicator_id', None)
    value_column = kwargs.get('value_column')
    base_filename = kwargs.get('base_filename', None)
    country_column = kwargs.get('country_column', None)
    key_column = kwargs.get('key_column', None)
    year = kwargs.get('year', None)
    group_column = kwargs.get('group_column', None)
    group_name = kwargs.get('group_name', None)
    aggregate = kwargs.get('aggregate', False)
    keep = kwargs.get('keep', 'last')
    return_dataframe = kwargs.get('return_dataframe', True)
    country_code_aggregate = kwargs.get('country_code_aggregate', False)
    region_column = kwargs.get('region_column', None)

    df.columns = df.columns.str.strip()
    df.dropna(inplace=True, axis=1, how="all")
    index_col = STANDARD_KEY_COLUMN if key_column is not None else STANDARD_COUNTRY_COLUMN
    if group_name is not None and group_column is not None:
        df = df.groupby(group_column).get_group(group_name)
        df.dropna(inplace=True, axis=1, how="all")

    indicator_rename = {value_column: await rename_indicator(indicator_id, year)}
    inverted_dictionary = await invert_dictionary(indicator_rename)
    indicator_cols = list(inverted_dictionary.keys())
    df.rename(columns=indicator_rename, inplace=True)
    if aggregate is False:
        df.drop_duplicates(subset=[index_col], keep=keep, inplace=True)
    elif aggregate is True:
        index_df = df.copy()
        group_df = index_df.groupby(index_col)
        df.set_index(index_col, inplace=True)
        for group in group_df.groups:
            temp_df = group_df.get_group(group)
            if len(temp_df) > 1:
                df.drop(group, inplace=True)
                temp_df.dropna(inplace=True, axis=1, how="all")
                temp_df = temp_df.sum()
                temp_df = temp_df.drop([index_col])
                df.at[group] = temp_df
        df.reset_index(inplace=True)
    if key_column is None:
        df = df[[STANDARD_COUNTRY_COLUMN] + indicator_cols]
        df = await add_country_code(df, country_name_column=country_column)
        # source_df.update(df)
        df = await add_region_code(source_df=df, region_name_col=country_column, region_key_col=region_column)
    if country_column is None:
        df = df[[STANDARD_KEY_COLUMN] + indicator_cols]
    else:
        pass
        df = await add_region_code(df, STANDARD_COUNTRY_COLUMN, region_column)
        df = df[[STANDARD_KEY_COLUMN, STANDARD_COUNTRY_COLUMN] + indicator_cols]
    if country_code_aggregate is True:
        copy_df = df.copy()
        key_group_df = copy_df.groupby(STANDARD_KEY_COLUMN)
        df.set_index(STANDARD_KEY_COLUMN, inplace=True)
        for group in key_group_df.groups:
            key_group = key_group_df.get_group(group)
            if len(key_group) > 1:
                df.drop(group, inplace=True)
                key_group.dropna(inplace=True, axis=1, how="all")
                key_group = key_group.sum()
                key_group = key_group.drop([STANDARD_KEY_COLUMN])
                df.at[group] = key_group
        df.reset_index(inplace=True)
    save_as = base_filename + ".csv"
    return await update_base_file(df=df, blob_name=save_as)


async def type3_transform(**kwargs):
    """
    Perform a type 3 transformation on the source DataFrame.

    Args:
        source_df (pd.DataFrame): Source DataFrame for transformation.
        indicator_id (str): Identifier for the indicator.
        value_column (str): Name of the column containing indicator values.
        base_filename (str): Base filename for saving the transformed data.
        country_column (str): Name of the country column.
        key_column (str): Name of the key column.
        datetime_column (str): Name of the datetime column.
        group_column (str): Name of the group column.
        group_name (str): Name of the group.
        aggregate (bool): Flag indicating whether to aggregate the data.
        aggregate_type (str): Type of aggregation (sum or mean).
        keep (str): Value to keep during aggregation.
        country_code_aggregate (bool): Flag indicating whether to aggregate by country code.
        return_dataframe (bool): Flag indicating whether to return the transformed DataFrame.
        region_column (str): Name of the region column.

    Returns:
        Transformed data uploaded to a blob as a CSV file.
    """

    df = kwargs.get("source_df")
    indicator_id = kwargs.get('indicator_id', None)
    value_column = kwargs.get('value_column')
    base_filename = kwargs.get('base_filename', None)
    country_column = kwargs.get('country_column', None)
    key_column = kwargs.get('key_column', None)
    datetime_column = kwargs.get('datetime_column', None)
    group_column = kwargs.get('group_column', None)
    group_name = kwargs.get('group_name', None)
    aggregate = kwargs.get('aggregate', False)
    aggregate_type = kwargs.get('aggregate_type', 'sum')
    keep = kwargs.get('keep', 'last')
    country_code_aggregate = kwargs.get('country_code_aggregate', False)
    return_dataframe = kwargs.get('return_dataframe', False)
    region_column = kwargs.get('region_column', None)

    index_col = key_column if key_column else country_column

    if group_name and group_column:
        # Filter the DataFrame based on the group name
        df = df.groupby(group_column).get_group(group_name)
        df.dropna(inplace=True, axis=1, how="all")

    if country_column and index_col != country_column:
        # Create a unique index DataFrame with country column if provided
        unique_index_df = df.drop_duplicates(subset=[index_col], keep="last")[[index_col, country_column]].set_index(
            index_col)
    else:
        unique_index_df = df.drop_duplicates(subset=[index_col], keep="last").set_index(index_col)

    df = df.groupby(index_col)
    indicator_cols = []

    for country in df.groups.keys():
        country_df = df.get_group(country)

        if aggregate:
            country_df.set_index(datetime_column, inplace=True)

            if aggregate_type == "sum":
                country_df = country_df.resample("Y").sum()
            elif aggregate_type == "mean":
                country_df = country_df.resample("Y").mean()

            country_df.reset_index(inplace=True)

        country_df["Year Column"] = country_df[datetime_column].apply(lambda x: x.year)
        country_df.drop_duplicates("Year Column", keep=keep, inplace=True)

        for index, row in country_df.iterrows():
            try:
                if await rename_indicator(indicator_id, row["Year Column"]) not in unique_index_df.columns:
                    unique_index_df.astype(
                        {await rename_indicator(indicator_id, row["Year Column"]): type(row[value_column])})
            except Exception as e:
                pass
            unique_index_df.at[country, await rename_indicator(indicator_id, row["Year Column"])] = row[value_column]
            indicator_cols.append(await rename_indicator(indicator_id, row["Year Column"]))

    indicator_cols = list(set(indicator_cols))
    unique_index_df.reset_index(inplace=True)

    if not key_column:
        # Rearrange columns and add country code and region code
        unique_index_df = unique_index_df[[country_column] + indicator_cols]
        unique_index_df = await add_country_code(unique_index_df, country_name_column=country_column)
        unique_index_df = await add_region_code(unique_index_df, country_column, region_column)
    elif not country_column:
        # Rearrange columns
        unique_index_df = unique_index_df[[key_column] + indicator_cols]
    else:
        # Rearrange columns, add region code, and rearrange columns again
        unique_index_df = await add_region_code(unique_index_df, country_column, region_column)
        unique_index_df = unique_index_df[[key_column, country_column] + indicator_cols]

    if country_code_aggregate:
        # Perform aggregation by country code
        index_df = unique_index_df.copy()
        group_df = index_df.groupby(key_column)
        unique_index_df.set_index(key_column, inplace=True)

        for group in group_df.groups:
            df = group_df.get_group(group)

            if len(df) > 1:
                unique_index_df.drop(group, inplace=True)
                df.dropna(inplace=True, axis=1, how="all")
                df = df.sum()
                df = df.drop([key_column])
                unique_index_df.at[group] = df

        unique_index_df.reset_index(inplace=True)

    return await update_base_file(df=unique_index_df, blob_name=base_filename + ".csv")
