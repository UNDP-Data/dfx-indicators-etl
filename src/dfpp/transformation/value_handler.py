"""handle transformations of string values to numeric values"""

import re
import pandas as pd

__all__ = ["handle_value"]


def handle_value(row: pd.Series) -> tuple[str | float, str]:
    """Process a row to coerce strings to numeric.
    If string value cannot be coerced return None

    Args:
        row (pd.Series): row of the indicator series to process, must contain `value` colum

    Returns:
        tuple[str | float, str]:
        value: None | float | int - value coerced to float,
        label:  None | str - label for values that contain comparision sign (original string value returned in such cases)
    """
    value = row["value"]

    def is_pure_zero_prefixed_number(value):
        """Check if the string is zero-prefixed and does not contain float separator"""
        return bool(re.match(r"0[0-9]*(\.[0-9]+)?$", value)) and not "." in value

    def try_convert_to_float(value):
        """Attempt to convert a string to a float after replacing ',' with empty string"""
        try:
            # substitute percentage symbol with empty string
            value = re.sub(r"\s*%\s*", "", value)

            # replace comma with empty string
            return float(value.replace(",", ""))
        except ValueError:
            return None

    def handle_comparison_string(value):
        """
        Handle cases where the string may contain only digits, optional dot or comma.
        Replace commas with "" if present. If alphabet symbols or invalid characters
        are present, return the original value. Handles inequality signs appropriately.
        """
        # remove comparison operators before converting
        cleaned_value = re.sub(r"[<>]=?", "", value)
        float_value = try_convert_to_float(cleaned_value)

        if float_value is not None:
            return (float_value, value)

        return None, None

    if isinstance(value, (int, float)):
        return value, None

    if isinstance(value, str):
        if is_pure_zero_prefixed_number(value):
            return None, None

        if any(c in value for c in ["<", ">"]):
            return handle_comparison_string(value)

        float_value = try_convert_to_float(value)

        return float_value, None

    return None, None
