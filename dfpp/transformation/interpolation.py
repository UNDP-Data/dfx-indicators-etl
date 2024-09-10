import pandas as pd
import numpy as np


def gappiness(
    df_long_group: pd.DataFrame, indicator_id: str
) -> pd.Series:
    """
    Calculates gappiness index for a single group of data.

    Parameters:
        df_long_group: pd.DataFrame - DataFrame with a single group of indicator data in long format
        indicator_id: str - name of the indicator to calculate gappiness index for

    Returns:
        pd.Series - Series with gappiness index and other relevant metrics
    """
    available_years = df_long_group[df_long_group[indicator_id].notna()]["year"].sort_values().values

    if len(available_years) == 0:
        return pd.Series(
            {
                "gappiness_index": np.nan,
                "observed_years": 0,
                "missing_years": np.nan,
                "year_min": np.nan,
                "year_max": np.nan,
            },
            dtype=object,
        )

    year_min = available_years.min()
    year_max = available_years.max()

    total_years = year_max - year_min + 1
    observed_years = len(available_years)
    missing_years = total_years - observed_years

    gaps = np.diff(available_years) - 1
    average_gap_size = gaps[gaps > 0].mean() if np.any(gaps > 0) else 0

    gappiness_index = 0
    if total_years > 0:
        gappiness_index = (missing_years / total_years) * (
            1 + np.log(1 + average_gap_size)
        )

    return pd.Series(
        {
            "gappiness_index": gappiness_index,
            "observed_years": observed_years,
            "missing_years": missing_years,
            "year_min": year_min,
            "year_max": year_max,
        },
        dtype=object,
    )


def calculate_gappiness_index(df_long: pd.DataFrame, indicator_id: str) -> pd.DataFrame:
    """
    Calculates gappiness index for each country.
    Parameters:
        df_long: pd.DataFrame - long format dataframe
        indicator_id: str - name of the indicator to calculate gappiness index for
    """
    return (
        df_long.groupby("alpha_3_code")
        .apply(gappiness, indicator_id=indicator_id)
        .reset_index()
    )


def interpolate_group(df_long_group: pd.DataFrame, indicator_id: str) -> pd.DataFrame:
    """Interpolates the data for a single group based on the Gappiness Index."""
    gappiness_index = df_long_group["gappiness_index"].iloc[0]
    observed_years = df_long_group["observed_years"].iloc[0]
    missing_years = df_long_group["missing_years"].iloc[0]

    year_min = df_long_group["year_min"].iloc[0]
    year_max = df_long_group["year_max"].iloc[0]

    df_long_group.sort_values("year", inplace=True)

    if not missing_years:
        return df_long_group

    if gappiness_index < 0.2:
        df_long_group[indicator_id] = df_long_group[indicator_id].interpolate(
            method="spline", order=3, limit_direction="forward", limit_area="inside"
        )
        return df_long_group
    if gappiness_index < 0.5:
        return df_long_group

    ffill_mask = df_long_group["year"].between(year_min, year_max, inclusive="both")

    df_long_group.loc[ffill_mask, indicator_id] = df_long_group.loc[ffill_mask, indicator_id].ffill()

    return df_long_group


def interpolate_data(
    df_long: pd.DataFrame, indicator_id: str, debug=False,
) -> pd.DataFrame:
    """Interpolate data based on Gappiness Index."""

    df_gappiness = calculate_gappiness_index(df_long, indicator_id)

    df_long = df_long.merge(
        df_gappiness[
            [
                "alpha_3_code",
                "gappiness_index",
                "observed_years",
                "missing_years",
                "year_min",
                "year_max",
            ]
        ],
        on="alpha_3_code",
    )

    df_long_interpolated = df_long.groupby("alpha_3_code").apply(
        interpolate_group, indicator_id=indicator_id
    )

    if not debug == True:
        return df_long_interpolated.drop(
            columns=[
                "gappiness_index",
                "observed_years",
                "missing_years",
                "year_min",
                "year_max",
            ]
        ).reset_index(drop=True)

    df_long_interpolated.reset_index(
        drop=True,
    )

    df_debug_interpolation = df_long.merge(
       df_long_interpolated, on=["alpha_3_code", "year"],
        suffixes=("_original", "_interpolated"),
    )

    return df_debug_interpolation
