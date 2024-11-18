import re
import pandas as pd
from enum import Enum


class ValueLabel(Enum):
    STRING_TO_FLOAT = "string to float64"
    INTEGER_TO_FLOAT = "integer to float64"
    FLOAT_TO_FLOAT = "float to float64"
    POSITIVE_INFINITY = 99999999
    NEGATIVE_INFINITY = -99999999
    STRING_ONLY = "string"


def handle_value(row: pd.Series) -> tuple[str | float, str]:
    """Process a row of data and return a tuple of the processed value and its label.
    Indicating whether value conversion to float64 can be done."""
    value = row["value"]

    def is_pure_zero_prefixed_number(string):
        """Check if the string is zero-prefixed and does not contain ',' or '.'."""
        return re.match(r"0[0-9]+$", string) and not any(
            c in string for c in [",", "."]
        )

    def try_convert_to_float(string_value):
        """Attempt to convert a string to a float after replacing ',' with '.'."""
        try:
            return (
                float(string_value.replace(",", ".")),
                ValueLabel.STRING_TO_FLOAT.value,
            )
        except ValueError:
            return None

    def handle_comparison_string(value):
        """Handle cases where the string contains comparison operators."""
        match = re.match(r"(-?[0-9,\.]+)?\s*([<>]=?|>>|<<)\s*(-?[0-9,\.]+)?", value)
        if match:
            pre_value = match.group(1)
            operator = match.group(2) if match.group(2) else ""
            post_value = match.group(3)

            string_value = post_value or pre_value
            if string_value:
                if is_pure_zero_prefixed_number(string_value):
                    return value, ValueLabel.STRING_ONLY.value

                float_conversion = try_convert_to_float(string_value)
                if float_conversion:
                    float_value, label = float_conversion
                    if ">" in operator:
                        return float_value, ValueLabel.POSITIVE_INFINITY.value
                    if "<" in operator:
                        return float_value, ValueLabel.NEGATIVE_INFINITY.value

                    return float_value, label

        return value, ValueLabel.STRING_ONLY.value

    def handle_numeric_string(value):
        """Extract and convert the numeric part of a string."""
        match = re.search(r"\d+(?:[.,]\d+)*", value)
        if match:
            string_value = match.group(0)
            if is_pure_zero_prefixed_number(string_value):
                return value, ValueLabel.STRING_ONLY.value

            float_conversion = try_convert_to_float(string_value)
            if float_conversion:
                return float_conversion

        return None

    # PROCESSING LOGIC
    if isinstance(value, str):
        if is_pure_zero_prefixed_number(value):
            return value, ValueLabel.STRING_ONLY.value

        if any(c in value for c in ["<", ">"]):
            return handle_comparison_string(value)

        numeric_result = handle_numeric_string(value)
        if numeric_result:
            return numeric_result

    try:
        float_value = float(value)
        return float_value, ValueLabel.INTEGER_TO_FLOAT.value
    except (ValueError, TypeError):
        pass

    return value, ValueLabel.STRING_ONLY.value
