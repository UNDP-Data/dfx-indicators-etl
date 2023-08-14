import asyncio
import io
import math
import os
import re
import logging
import numpy as np
import pandas as pd

from dfpp.constants import STANDARD_KEY_COLUMN, CURRENT_YEAR
from dfpp.storage import StorageManager
from dfpp.utils import interpolate_data

logger = logging.getLogger(__name__)


# The function generates a population dataframe and calculates regional population totals.
async def region_population_mapping(population_df: pd.DataFrame, grouped_region):
    """
    Generates a population dataframe with interpolated values and calculates regional population totals.

    Args:
        grouped_region (DataFrameGroupBy): A grouped dataframe representing regions.

    Returns:
        tuple: A tuple containing the population dataframe and a dictionary of regional population totals.
        :param population_df:
        :param grouped_region:
        :param storage_manager:
    """

    population_df = population_df.reindex(sorted(population_df.columns), axis=1)

    # Iterate through rows in the population dataframe
    for index, row in population_df.iterrows():
        # Create a new dataframe with the current row
        co_df = pd.DataFrame([row])
        co_df = co_df.transpose()

        # Reset the index and add a 'year' column
        co_df.reset_index(inplace=True)
        co_df['year'] = (co_df['index'].apply(lambda date_string: date_string.replace('totalpopulation_untp_', '')))
        co_df = co_df.iloc[1:]

        # Interpolate data for population values
        pop_years, pod_values, cleaned_df = await interpolate_data(data_frame=co_df[['year', index]],
                                                                   target_column=index)
        for idx, year in enumerate(pop_years):
            col = 'totalpopulation_untp_' + str(year)
            if col not in population_df.columns:
                population_df[col] = np.nan
            population_df.loc[index][col] = pod_values[idx]

    region_population = {}

    # Set the index of the population dataframe
    population_df.set_index(STANDARD_KEY_COLUMN, inplace=True)

    # Iterate through grouped regions and calculate regional population totals
    for group_name, df_group in grouped_region:
        country_list = df_group['iso3'].tolist()
        for year in range(2000, CURRENT_YEAR):
            if f"totalpopulation_untp_{year}" in population_df.columns:
                region_population[f"{group_name}_{str(year)}"] = population_df[population_df.index.isin(country_list)][
                    f"totalpopulation_untp_{str(year)}"].sum()
            else:
                print('Not found ', "totalpopulation_untp_" + str(year), group_name)

    # Reset the index of the population dataframe and return results
    population_df.reset_index(inplace=True)
    population_df.set_index(STANDARD_KEY_COLUMN, inplace=True)
    return region_population


async def generate_region_aggregates(storage_manager: StorageManager, indicator_id: str, grouped_region,
                                     population_df: pd.DataFrame,
                                     output_df: pd.DataFrame, region_population):
    """
    Process indicator data for grouped regions.

    Args:
        storage_manager (StorageManager): An instance of StorageManager for data retrieval.
        indicator_id (str): The ID of the indicator to be processed.
        grouped_region (iterable): Grouped regions and their dataframes.
        population_df (pd.DataFrame): DataFrame containing population data.
        output_df (pd.DataFrame): DataFrame containing indicator data.
        region_population (dict): Region-wise population data.

    Returns:
        pd.DataFrame, pd.DataFrame: Aggregated data and individual country data DataFrames.
    """

    print(output_df.columns.to_list())
    # Retrieve indicator configurations
    indicator_configurations = await storage_manager.get_indicators_cfg(indicator_ids=[indicator_id])
    indicator_config = indicator_configurations[0]['indicator']

    aggregated_data_list = []
    individual_country_data_list = []

    # Skip indicators with missing AggregateType
    if not indicator_config["aggregate_type"]:
        logger.warning(f'Skipping indicator {indicator_id} with missing aggregate_type')
        return None

    denominator_indicator_name = str(indicator_config["denominator_indicator_id"])

    minimum_year = None

    # Initialize per capita factor
    per_capita_factor = 1
    if indicator_config['per_capita'] != "None":
        per_capita_factor = 1 / float(indicator_config['per_capita'])

    # Regular expression pattern for column matching
    indicator_column_prefix = '^' + re.escape(indicator_id) + '_2'

    denominator_df = None
    denominator_transformed_df = None
    region_denominator_df = None

    if denominator_indicator_name != '':
        denominator_column_prefix = '^' + re.escape(denominator_indicator_name) + '_2'
        denominator_df = output_df.filter(regex=denominator_column_prefix, axis=1)

    if indicator_config['min_year'] != "None" and indicator_config['min_year'] != '':
        minimum_year = str(indicator_config['min_year'])
    print("XXXX")
    print(denominator_df.head())
    print(indicator_column_prefix)
    indicator_df = output_df.filter(regex=indicator_column_prefix, axis=1)
    print("XXXX")
    print(indicator_df.head())
    print("XXXX")
    all_columns = indicator_df.columns.to_list()
    all_columns.append(STANDARD_KEY_COLUMN)

    total_original_data_count = {}

    # Iterate through grouped regions
    for group_name, region_df in grouped_region:
        country_iso3_list = region_df['iso3'].tolist()
        region_indicator_df = indicator_df[output_df.index.isin(country_iso3_list)]
        region_indicator_df = region_indicator_df.reindex(sorted(region_indicator_df.columns), axis=1)

        if denominator_df is not None:
            region_denominator_df = denominator_df[output_df.index.isin(country_iso3_list)]
            region_denominator_df = region_denominator_df.reindex(sorted(region_denominator_df.columns), axis=1)

        for year in range(2000, CURRENT_YEAR):
            indicator_column = (indicator_id + '_' + str(year))
            if indicator_column not in region_indicator_df.columns:
                region_indicator_df[indicator_column] = np.NaN

            if denominator_df is not None and (
                    denominator_indicator_name + '_' + str(year)) not in region_denominator_df.columns:
                region_denominator_df[denominator_indicator_name + '_' + str(year)] = np.NaN

        # Transform data for further processing
        transformed_df = region_indicator_df.transpose()
        transformed_df.reset_index(inplace=True)
        transformed_df['year'] = (
            transformed_df['index'].apply(lambda date_string: date_string.replace(indicator_id + '_', '')))

        denominator_transformed_df = None
        if region_denominator_df is not None:
            denominator_transformed_df = region_denominator_df.transpose()
            denominator_transformed_df.reset_index(inplace=True)
            denominator_transformed_df['year'] = (
                denominator_transformed_df['index'].apply(
                    lambda date_string: date_string.replace(denominator_indicator_name + '_', '')))

        denominator_data = None
        if denominator_transformed_df is not None:
            denominator_data = {}

        indicator_data = {}

        # Iterate through columns in transformed data
        for col in transformed_df.columns:
            if col != 'year' and col != 'index':
                year_col_df = transformed_df[['year', col]]
                denominator_values = {}
                if denominator_transformed_df is not None:
                    denominator_year_col_df = denominator_transformed_df[['year', col]]
                    interpolated_years_d, interpolated_values_d, cleaned_df_d = await interpolate_data(
                        denominator_year_col_df, col, minimum_year)
                    if interpolated_years_d is None:
                        continue

                    denominator_year_col_df.set_index('year', inplace=True)

                    for i, interpolated_value_d in enumerate(interpolated_values_d):
                        denominator_values[interpolated_years_d[i]] = pd.to_numeric(interpolated_value_d)
                        denominator_year_col_df.loc[interpolated_years_d[i]] = pd.to_numeric(interpolated_value_d)

                    year_col_df.set_index('year', inplace=True)
                    transformed_indicator_df = year_col_df.select_dtypes(exclude=['object', 'datetime']) * float(
                        per_capita_factor)
                    transformed_indicator_df.index = pd.to_numeric(transformed_indicator_df.index)
                    year_col_df = transformed_indicator_df.mul(denominator_year_col_df, axis=0)
                    year_col_df.reset_index(inplace=True)

                interpolated_years, interpolated_values, cleaned_df = await interpolate_data(year_col_df, col,
                                                                                             minimum_year)
                for i2, row2 in cleaned_df.iterrows():
                    if total_original_data_count.get(group_name + '_' + str(row2['year'])) is None:
                        total_original_data_count[group_name + '_' + str(row2['year'])] = 0
                    country_population = population_df.loc[col]
                    if country_population is not None:
                        total_original_data_count[group_name + '_' + str(row2['year'])] += country_population.get(
                            "totalpopulation_untp_" + str(row2['year']), 0)
                for i, interpolated_year in enumerate(interpolated_years):
                    interpolated_year = int(interpolated_year)
                    if indicator_data.get(indicator_id + '_' + str(interpolated_year)) is None:
                        indicator_data[indicator_id + '_' + str(interpolated_year)] = 0
                    if denominator_data is not None and denominator_data.get(
                            indicator_id + '_' + str(interpolated_year)) is None:
                        denominator_data[indicator_id + '_' + str(interpolated_year)] = 0
                    if indicator_config["aggregate_type"] == 'PositiveSum' and interpolated_values[i] <= 0:
                        interpolated_values[i] = 0
                        if denominator_data is not None:
                            denominator_data[indicator_id + '_' + str(interpolated_year)] += denominator_values.get(
                                interpolated_year)
                    elif denominator_transformed_df is not None:
                        if denominator_values.get(interpolated_year) is not None:
                            denominator_data[indicator_id + '_' + str(interpolated_year)] += denominator_values.get(
                                interpolated_year)
                            indicator_data[indicator_id + '_' + str(interpolated_year)] += round(interpolated_values[i],
                                                                                                 5)
                    else:
                        indicator_data[indicator_id + '_' + str(interpolated_year)] += round(interpolated_values[i], 5)

                    individual_country_data_list.append({
                        'year': interpolated_year,
                        'iso3': col,
                        'region': group_name,
                        'indicatorid': indicator_id,
                        'value': round(interpolated_values[i], 5),
                        'denominator': denominator_values.get(interpolated_year)
                    })
        if denominator_data is not None:
            for denominator_key in denominator_data:
                if denominator_key != STANDARD_KEY_COLUMN:
                    if denominator_data[denominator_key] == 0:
                        indicator_data[denominator_key] = 0
                    else:
                        indicator_data[denominator_key] = round(
                            (indicator_data[denominator_key] / (denominator_data[denominator_key] * per_capita_factor)),
                            5)

        for indicator_key in indicator_data:
            year_str = indicator_key.replace(indicator_id + '_', '')

            total_population = region_population.get(group_name + '_' + year_str, 0)
            original_data_count = total_original_data_count.get(group_name + '_' + year_str, 0)

            if total_population == 0:
                print('Population is zero for', indicator_id, year_str, group_name)
                continue

            percentage = original_data_count * 100 / total_population
            if percentage >= 50:
                aggregated_data_list.append({
                    'region': group_name,
                    'indicatorid': indicator_id,
                    'year': year_str,
                    'value': indicator_data[indicator_key]
                })

    # Create DataFrames for aggregated and individual country data
    aggregated_data_df = pd.DataFrame(aggregated_data_list)
    individual_country_data_df = pd.DataFrame(individual_country_data_list)
    return aggregated_data_df, individual_country_data_df


async def aggregate_indicator(project: str = None, indicator_id: str = None):
    # output_df = pd.read_csv('/home/thuha/Downloads/output (2).csv')
    # output_df.set_index(STANDARD_KEY_COLUMN, inplace=True)
    # undp_region_df = pd.read_excel('/home/thuha/Downloads/country_lookup.xlsx', sheet_name='undp_region_taxonomy')

    async with StorageManager() as storage_manager:
        output_bytes = await storage_manager.cached_download(
            source_path=os.path.join(storage_manager.OUTPUT_PATH, project, "output_test.csv")
        )
        output_df = pd.read_csv(io.BytesIO(output_bytes), index_col=STANDARD_KEY_COLUMN)
        print(output_df.head())
        output_df = output_df.pivot(columns=["indicator_id", 'year'], values='value')
        # Flatten the column MultiIndex
        output_df.columns = [f'{col[0]}_{col[1]}' for col in output_df.columns]

        undp_region_df = pd.read_excel(io.BytesIO(await storage_manager.cached_download(
            source_path=os.path.join(storage_manager.UTILITIES_PATH, "country_lookup.xlsx")
        )), sheet_name='undp_region_taxonomy')
        undp_region_df.dropna(subset=['region_old'], inplace=True)
        grouped_region = undp_region_df.groupby('region')
        population_df = pd.read_csv(io.BytesIO(await storage_manager.cached_download(
            source_path=os.path.join(storage_manager.UTILITIES_PATH, "population.csv")
        )))
        region_population = await region_population_mapping(population_df=population_df, grouped_region=grouped_region)
        aggregated_data_df, individual_country_data_df = await generate_region_aggregates(
            storage_manager=storage_manager,
            indicator_id=indicator_id,
            grouped_region=grouped_region,
            population_df=population_df,
            output_df=output_df,
            region_population=region_population
        )
        print("EEEEEEEEEEEEEEENNNNNNNNNNNNNNNNDDDDDDDDDDD")
        print(aggregated_data_df.head())

        #
        # print(aggregated_data_df.head())


if __name__ == "__main__":
    print("Running publish.py")
    asyncio.run(aggregate_indicator(project="access_all_data", indicator_id="populationlivinginslums_cpiaplis"))
