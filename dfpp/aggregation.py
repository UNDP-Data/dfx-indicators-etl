import asyncio
import math
import re
import logging
import numpy as np
import pandas as pd

from dfpp.constants import STANDARD_KEY_COLUMN, CURRENT_YEAR
from dfpp.storage import StorageManager
from dfpp.utils import interpolate_data


logger = logging.getLogger(__name__)


async def generate_population_dataframe():
    output_df = pd.read_csv('/home/thuha/Downloads/output (2).csv')
    population_df = output_df.filter(regex=STANDARD_KEY_COLUMN + '|^totalpopulation_untp_2', axis=1)
    population_df = population_df.reindex(sorted(population_df.columns), axis=1)
    for index, row in population_df.iterrows():
        co_df = pd.DataFrame([row])
        co_df = co_df.transpose()
        co_df.reset_index(inplace=True)
        co_df['year'] = (co_df['index'].apply(lambda date_string: date_string.replace('totalpopulation_untp_', '')))
        co_df = co_df.iloc[1:]
        pop_years, pod_values, cleaned_df = await interpolate_data(data_frame=co_df[['year', index]],
                                                                   target_column=index)
        for idx, year in enumerate(pop_years):
            col = 'totalpopulation_untp_' + str(year)
            if col not in population_df.columns:
                population_df[col] = np.nan
            population_df.loc[index][col] = pod_values[idx]
    region_population = {}
    undp_region_df = pd.read_excel('/home/thuha/Downloads/country_lookup.xlsx', sheet_name='undp_region_taxonomy')
    undp_region_df.dropna(subset=['region_old'], inplace=True)
    grouped_region = undp_region_df.groupby('region')
    for group_name, df_group in grouped_region:
        country_list = df_group['iso3'].tolist()
        for y in range(2000, CURRENT_YEAR):
            if "totalpopulation_untp_" + str(y) in population_df.columns:
                region_population[group_name + '_' + str(y)] = population_df[population_df.index.isin(country_list)][
                    "totalpopulation_untp_" + str(y)].sum()
            else:
                print('Not found ', "totalpopulation_untp_" + str(y), group_name)
    population_df.reset_index(inplace=True)
    population_df.set_index(STANDARD_KEY_COLUMN, inplace=True)
    return population_df, region_population


async def generate_region_aggregates(storage_manager: StorageManager, indicator_id, grouped_region, population_df,
                                     output_df,
                                     region_population):
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

    indicator_cfgs = await storage_manager.get_indicators_cfg(indicator_ids=[indicator_id])
    cfg = indicator_cfgs[0]
    indicator_cfg = cfg['indicator']

    all_agg_data = []
    # List to store data for individual countries
    all_country_data = []

    # Skip rows with missing AggregateType
    if indicator_cfg["aggregate_type"] is None or str(indicator_cfg["aggregate_type"]) == '':
        logger.warning(f'Skipping indicator {indicator_id} with missing aggregate_type')
        return None

    denominator_name = str(indicator_cfg["denominator_indicator_id"])
    minyear = None

    # Initialize perCapita factor
    perCapita = 1
    if pd.notnull(indicator_cfg['per_capita']) and not math.isnan(indicator_cfg['per_capita']):
        perCapita = 1 / float(indicator_cfg['per_capita'])

    # Regular expression pattern for column matching
    prefix = '^' + re.escape(indicator_id) + '_2'

    df_d = None
    df_trans_d = None

    if denominator_name != '':
        prefix_d = '^' + re.escape(denominator_name) + '_2'
        df_d = output_df.filter(regex=prefix_d, axis=1)

    if pd.notnull(indicator_cfg['min_year']):
        minyear = indicator_cfg['min_year']

    df_i = output_df.filter(regex=prefix, axis=1)

    all_cols = df_i.columns.to_list()
    all_cols.append(STANDARD_KEY_COLUMN)

    total_original_data_count = {}

    # Iterate through grouped regions
    for group_name, df_group in grouped_region:
        country_list = df_group['iso3'].tolist()

        df_r_i = df_i[output_df.index.isin(country_list)]
        df_r_i = df_r_i.reindex(sorted(df_r_i.columns), axis=1)

        if df_d is not None:
            df_r_d = df_d[output_df.index.isin(country_list)]
            df_r_d = df_r_d.reindex(sorted(df_r_d.columns), axis=1)

        for y in range(2000, CURRENT_YEAR):
            col = (indicator_id + '_' + str(y))

            if col not in df_r_i.columns:
                df_r_i[col] = np.NaN

            if df_d is not None and (denominator_name + '_' + str(y)) not in df_r_d.columns:
                df_r_d[denominator_name + '_' + str(y)] = np.NaN

        # Transform data for further processing
        df_trans = df_r_i.transpose()
        df_trans.reset_index(inplace=True)
        df_trans['year'] = (
            df_trans['index'].apply(lambda date_string: date_string.replace(indicator_id + '_', '')))

        if df_d is not None:
            df_trans_d = df_r_d.transpose()
            df_trans_d.reset_index(inplace=True)
            df_trans_d['year'] = (
                df_trans_d['index'].apply(lambda date_string: date_string.replace(denominator_name + '_', '')))

        obj_d = None
        if df_trans_d is not None:
            obj_d = {}

        obj = {}

        # Iterate through columns in transformed data
        for col in df_trans.columns:
            if col != 'year' and col != 'index':
                dfn = df_trans[['year', col]]

                d_values = {}
                if df_trans_d is not None:
                    dfn_d = df_trans_d[['year', col]]
                    # Interpolate denominator data
                    interpolated_years_d, interpolated_values_d, df_cleaned = interpolate_data(dfn_d, col, minyear)
                    if interpolated_years_d is None:
                        continue

                    dfn_d.set_index('year', inplace=True)

                    for i, y in enumerate(interpolated_years_d):
                        d_values[y] = pd.to_numeric(interpolated_values_d[i])
                        dfn_d.loc[y] = pd.to_numeric(interpolated_values_d[i])

                    dfn.set_index('year', inplace=True)
                    df_trans1 = dfn.select_dtypes(exclude=['object', 'datetime']) * float(perCapita)
                    df_trans1.index = pd.to_numeric(df_trans1.index)
                    dfn = df_trans1.mul(dfn_d, axis=0)
                    dfn.reset_index(inplace=True)

                # Interpolate main data
                interpolated_years, interpolated_values, df_cleaned = interpolate_data(dfn, col, minyear)

                for i2, r2 in df_cleaned.iterrows():
                    if total_original_data_count.get(group_name + '_' + str(r2['year'])) is None:
                        total_original_data_count[group_name + '_' + str(r2['year'])] = 0

                    c_pop = population_df.loc[col]
                    if c_pop is not None:
                        total_original_data_count[group_name + '_' + str(r2['year'])] += c_pop.get(
                            "totalpopulation_untp_" + str(r2['year']), 0)

                for i, y in enumerate(interpolated_years):
                    y = int(y)
                    if obj.get(indicator_id + '_' + str(y)) is None:
                        obj[indicator_id + '_' + str(y)] = 0
                    if obj_d is not None and obj_d.get(indicator_id + '_' + str(y)) is None:
                        obj_d[indicator_id + '_' + str(y)] = 0
                    if indicator_cfg["AggregateType"] == 'PositiveSum' and interpolated_values[i] <= 0:
                        interpolated_values[i] = 0
                        if obj_d is not None:
                            obj_d[indicator_id + '_' + str(y)] += d_values.get(y)
                    elif df_trans_d is not None:
                        if d_values.get(y) is not None:
                            obj_d[indicator_id + '_' + str(y)] += d_values.get(y)
                            obj[indicator_id + '_' + str(y)] += round(interpolated_values[i], 5)
                    else:
                        obj[indicator_id + '_' + str(y)] += round(interpolated_values[i], 5)

                    all_country_data.append({
                        'year': y,
                        'iso3': col,
                        'region': group_name,
                        'indicatorid': indicator_id,
                        'value': round(interpolated_values[i], 5),
                        'denominator': d_values.get(y)
                    })

        if obj_d is not None:
            for k in obj_d:
                if k != STANDARD_KEY_COLUMN:
                    if obj_d[k] == 0:
                        obj[k] = 0
                    else:
                        obj[k] = round((obj[k] / (obj_d[k] * perCapita)), 5)

        for k in obj:
            x = k.replace(indicator_id + '_', '')

            tot = region_population.get(group_name + '_' + x, 0)
            original = total_original_data_count.get(group_name + '_' + x, 0)

            if tot == 0:
                print('Pop zero', indicator_id, x, group_name)
                continue

            p = original * 100 / tot
            if p >= 50:
                all_agg_data.append({
                    'region': group_name,
                    'indicatorid': indicator_id,
                    'year': x,
                    'value': obj[k]
                })
    # Create DataFrames for aggregated and individual country data
    df_final = pd.DataFrame(all_agg_data)
    df_final_country = pd.DataFrame(all_country_data)
    return df_final, df_final_country


# async def delete_dirs():
#     for file in glob.glob('*.csv'):
#         os.remove(file)

if __name__ == "__main__":
    print("Running publish.py")
    asyncio.run(generate_population_dataframe())
    # asyncio.run(delete_dirs())