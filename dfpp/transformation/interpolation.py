import pandas as pd
import numpy as np


def gappiness(group, indicator_id):
    available_years = group[group[indicator_id].notna()]["year"].sort_values().values

    if len(available_years) == 0:
        return pd.Series(
            {
                "gappiness_index": np.nan,
                "observed_years": 0,
                "missing_years": np.nan,
                "year_min": np.nan,
                "year_max": np.nan,
            }
        )

    year_min = available_years.min()
    year_max = available_years.max()

    total_years = year_max - year_min + 1
    observed_years = len(available_years)
    missing_years = total_years - observed_years

    gaps = np.diff(available_years) - 1
    average_gap_size = gaps[gaps > 0].mean() if np.any(gaps > 0) else 0

    if total_years > 0:
        gappiness_index = (missing_years / total_years) * (
            1 + np.log(1 + average_gap_size)
        )
    else:
        gappiness_index = 0

    return pd.Series(
        {
            "gappiness_index": gappiness_index,
            "observed_years": observed_years,
            "missing_years": missing_years,
            "year_min": year_min,
            "year_max": year_max,
        }
    )


def calculate_gappiness_index(long_df: pd.DataFrame, indicator_id: str) -> pd.DataFrame:
    """
    Calculates gappiness index for each country.
    Parameters:
        long_df: pd.DataFrame - long format dataframe
        indicator_id: str - name of the indicator to calculate gappiness index for
    """
    return (
        long_df.groupby("Alpha-3 code")
        .apply(gappiness, indicator_id=indicator_id)
        .reset_index()
    )


def interpolate_group(group: pd.DataFrame, indicator_id: str) -> pd.DataFrame:
    """Interpolates the data for a single group based on the Gappiness Index."""
    gappiness_index = group["gappiness_index"].iloc[0]
    observed_years = group["observed_years"].iloc[0]
    missing_years = group["missing_years"].iloc[0]

    if not missing_years:
        return group

    if gappiness_index < 0.2:
        group[indicator_id] = group[indicator_id].interpolate(
            method="spline", order=3, limit_direction="forward", limit_area="inside"
        )
        return group
    if gappiness_index < 0.5:

        group[indicator_id] = group[indicator_id].interpolate(
            method="spline", order=3, limit_direction="forward", limit_area="inside"
        )
        return group

    group[indicator_id] = group[indicator_id].fillna(
        method="ffill",
    )

    return group


def interpolate_data(long_df: pd.DataFrame, indicator_id: str) -> pd.DataFrame:
    """Interpolate data based on Gappiness Index."""

    gappiness_df = calculate_gappiness_index(long_df, indicator_id)

    long_df = long_df.merge(
        gappiness_df[
            ["Alpha-3 code", "gappiness_index", "observed_years", "missing_years"]
        ],
        on="Alpha-3 code",
    )

    interpolated_long_df = long_df.groupby("Alpha-3 code").apply(
        interpolate_group, indicator_id=indicator_id
    )

    return interpolated_long_df.drop(
        columns=["gappiness_index", "observed_years", "missing_years"]
    ).reset_index(drop=True)
