import re
import pandas as pd
from enum import Enum


class ValueLabel(Enum):
    POSITIVE_INFINITY = 99999999
    NEGATIVE_INFINITY = -99999999

def handle_value(row: pd.Series) -> tuple[str | float, str]:
    """Process a row of data and return a tuple of the processed value and its label.
    Indicating whether value conversion to float64 can be done."""
    value = row["value"]

    def is_pure_zero_prefixed_number(value):
        """Check if the string is zero-prefixed and does not contain float separator"""
        return bool(re.match(r"0[0-9]*(\.[0-9]+)?$", value)) and not "." in value
    
    def try_convert_to_float(value):
        """Attempt to convert a string to a float after replacing ',' with '.'"""
        if re.search(r"\d[\+\-\*/]\d", value):
            return None
        try:
            return float(value.replace(",", ""))
        except ValueError:
            return None

    def handle_comparison_string(value):
        """
        Handle cases where the string may contain only digits, optional dot or comma.
        Replace commas with "" if present. If alphabet symbols or invalid characters
        are present, return the original value. Handles inequality signs appropriately.
        """
        if re.search(r"[a-zA-Z]", value):
            return None, None

        cleaned_value = re.sub(r"[<>]=?", "", value).replace(",", "") 
        float_value = try_convert_to_float(cleaned_value)
        
        if ">" in value:
            return (float_value, ValueLabel.POSITIVE_INFINITY)

        elif "<" in value:
            return (float_value, ValueLabel.NEGATIVE_INFINITY)
        return None, None

    if isinstance(value, str):
        if is_pure_zero_prefixed_number(value):
            return None, None

        if any(c in value for c in ["<", ">"]):
            return handle_comparison_string(value)

        numeric_result = try_convert_to_float(value)
        if numeric_result:
            return numeric_result, None

    try:
        float_value = float(value)
        return float_value, None
    except (ValueError, TypeError):
        pass

    return None, None
