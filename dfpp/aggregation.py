import asyncio
import io
import os
import re
import logging
from typing import Any

import numpy as np
import pandas as pd

from dfpp.constants import STANDARD_KEY_COLUMN, CURRENT_YEAR
from dfpp.utils import base_df_for_indicator
from dfpp.storage import StorageManager
from dfpp.utils import interpolate_data

logger = logging.getLogger(__name__)


# The function generates a population dataframe and calculates regional population totals.
async def region_population_mapping(population_df: pd.DataFrame, grouped_region: Any):
    """
    Generates a population dataframe with interpolated values and calculates regional population totals.

    Args:
        grouped_region (DataFrameGroupBy): A grouped dataframe representing regions.
        population_df (pd.DataFrame): DataFrame containing population data.
    Returns:
        tuple: A tuple containing the population dataframe and a dictionary of regional population totals.
    """
    try:
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
        # population_df.set_index(STANDARD_KEY_COLUMN, inplace=True)

        # Iterate through grouped regions and calculate regional population totals
        for group_name, df_group in grouped_region:
            country_list = df_group['iso3'].tolist()
            for year in range(2000, CURRENT_YEAR):
                if f"totalpopulation_untp_{year}" in population_df.columns:
                    region_population[f"{group_name}_{str(year)}"] = \
                        population_df[population_df.index.isin(country_list)][
                            f"totalpopulation_untp_{str(year)}"].sum()
                else:
                    logger.info(f"Not found totalpopulation_untp_{str(year)}, {group_name}")

        # Reset the index of the population dataframe and return results
        population_df.reset_index(inplace=True)
        population_df.set_index(STANDARD_KEY_COLUMN, inplace=True)
        return region_population
    except Exception as e:
        raise e


async def generate_region_aggregates(storage_manager: StorageManager,
                                     indicator_id: str,
                                     grouped_region,
                                     population_df: pd.DataFrame,
                                     region_population,
                                     project: str):
    """
    Process indicator data for grouped regions.

    Args:
        storage_manager (StorageManager): An instance of StorageManager for data retrieval.
        indicator_id (str): The ID of the indicator to be processed.
        grouped_region (pd.DataFrameGroupBy): Grouped regions and their dataframes.
        population_df (pd.DataFrame): DataFrame containing population data.
        region_population (dict): Region-wise population data mapping .
        project (str): The project identifier.
    Returns:
        pd.DataFrame, pd.DataFrame: Aggregated data and individual country data DataFrames.
    """
    try:
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

        # Initialize minimum year
        minimum_year = None

        # Initialize per capita factor
        per_capita_factor = 1
        if indicator_config['per_capita'] != "None":
            per_capita_factor = 1 / float(indicator_config['per_capita'])

        # Regular expression pattern for column matching
        indicator_column_prefix = f'^{re.escape(indicator_id)}_2'

        denominator_df = None
        denominator_transformed_df = None
        region_denominator_df = None

        if denominator_indicator_name != "None" and denominator_indicator_name != "":
            denominator_column_prefix = '^' + re.escape(denominator_indicator_name) + '_2'
            denominator_base_df = await base_df_for_indicator(storage_manager=storage_manager,
                                                              indicator_id=denominator_indicator_name, project=project)
            denominator_base_df.set_index(STANDARD_KEY_COLUMN, inplace=True)
            denominator_base_df = denominator_base_df.drop(columns=['index'])
            denominator_df = denominator_base_df.filter(regex=denominator_column_prefix, axis=1)
        if indicator_config['min_year'] != "None" and indicator_config['min_year'] != '':
            minimum_year = str(indicator_config['min_year'])

        base_file_df = await base_df_for_indicator(storage_manager=storage_manager, indicator_id=indicator_id,
                                                   project=project)
        base_file_df.set_index(STANDARD_KEY_COLUMN, inplace=True)
        base_file_df = base_file_df.drop(columns=['index'])
        indicator_df = base_file_df.filter(regex=indicator_column_prefix, axis=1)

        all_columns = indicator_df.columns.to_list()
        all_columns.append(STANDARD_KEY_COLUMN)

        total_original_data_count = {}

        # Iterate through grouped regions
        for group_name, region_df in grouped_region:
            country_iso3_list = region_df['iso3'].tolist()
            region_indicator_df = indicator_df[base_file_df.index.isin(country_iso3_list)]
            region_indicator_df = region_indicator_df.reindex(sorted(region_indicator_df.columns), axis=1)

            if denominator_df is not None:
                region_denominator_df = denominator_df[denominator_base_df.index.isin(country_iso3_list)]
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
                                indicator_data[indicator_id + '_' + str(interpolated_year)] += round(
                                    interpolated_values[i],
                                    5)
                        else:
                            indicator_data[indicator_id + '_' + str(interpolated_year)] += round(interpolated_values[i],
                                                                                                 5)

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
                                (indicator_data[denominator_key] / (
                                        denominator_data[denominator_key] * per_capita_factor)),
                                5)

            for indicator_key in indicator_data:
                year_str = indicator_key.replace(indicator_id + '_', '')

                total_population = region_population.get(group_name + '_' + year_str, 0)
                original_data_count = total_original_data_count.get(group_name + '_' + year_str, 0)

                if total_population == 0:
                    logger.info(f'Population is zero for {indicator_id}, {year_str}, {group_name}')
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
    except Exception as e:
        raise e


async def aggregate_indicator(project: str = None, indicator_id: str = None):
    """
    Aggregates indicator data for regions and performs transformations.

    Args:
        project (str, optional): The project identifier. Defaults to None.
        indicator_id (str, optional): The indicator identifier. Defaults to None.

    Returns:
        json: A JSON string containing the aggregated data.
    """
    logger.info(f'Starting aggregation process for indicator_id {indicator_id}')
    try:
        # Establish a connection to the storage manager
        async with StorageManager() as storage_manager:
            # Download the output data file for the specified project
            output_bytes = await storage_manager.cached_download(
                source_path=os.path.join(storage_manager.OUTPUT_PATH, project, "output_test.csv")
            )
            # # Load the output data into a DataFrame
            # output_df = pd.read_csv(io.BytesIO(output_bytes), index_col=STANDARD_KEY_COLUMN)
            #
            # # Pivot the DataFrame to transform data into a suitable format for aggregation
            # output_df = output_df.pivot(columns=["indicator_id", 'year'], values='value')
            # output_df.columns = [f'{col[0]}_{col[1]}' for col in output_df.columns]

            # Read the UNDP region lookup data
            undp_region_df = pd.read_excel(io.BytesIO(await storage_manager.cached_download(
                source_path=os.path.join(storage_manager.UTILITIES_PATH, "country_lookup.xlsx")
            )), sheet_name='undp_region_taxonomy')
            undp_region_df.dropna(subset=['region_old'], inplace=True)
            grouped_region = undp_region_df.groupby('region')

            # Read population data
            population_df = pd.read_csv(io.BytesIO(await storage_manager.cached_download(
                source_path=os.path.join(storage_manager.UTILITIES_PATH, "population.csv")
            )))
            population_df = population_df.reindex(sorted(population_df.columns), axis=1)
            population_df.set_index(STANDARD_KEY_COLUMN, inplace=True)

            # Map region to population data
            region_population = await region_population_mapping(population_df=population_df,
                                                                grouped_region=grouped_region)

            # Generate aggregated data for regions
            aggregated_data_df, individual_country_data_df = await generate_region_aggregates(
                storage_manager=storage_manager,
                indicator_id=indicator_id,
                grouped_region=grouped_region,
                population_df=population_df,
                region_population=region_population,
                project=project
            )

            # Round the 'value' column and create a new column 'final_value'
            aggregated_data_df['final_value'] = aggregated_data_df['value'].round(2)

            # Create a new column 'ind_y' combining indicator ID and year
            new_col = 'ind_y'
            aggregated_data_df[new_col] = aggregated_data_df['indicatorid'] + '_' + aggregated_data_df['year']

            # Pivot the data to a new format using 'region' and 'ind_y' as indices
            agg_df_new_format = aggregated_data_df.pivot(index='region', columns='ind_y', values='final_value')
            logger.info(f'Completed aggregation process for indicator_id {indicator_id}')
            agg_df_new_format.to_csv(f'/home/thuha/Desktop/indicators_aggregates/{indicator_id}.csv')
            return agg_df_new_format.to_json(orient="records")
    except Exception as e:
        raise e


if __name__ == "__main__":
    pass
    # asyncio.run(aggregate_indicator(project="access_all_data", indicator_id="populationusingbasicdrinkingwater_cpiapbd"))
