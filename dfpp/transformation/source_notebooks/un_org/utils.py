import pandas as pd

__all__ = ["flatten_dict"]
def flatten_dict(data: list[dict]) -> pd.DataFrame:
    """
    Flattens the attributes or dimension dictionary into a pandas DataFrame with each row containing
    an key of the attribute/dimension id, code, and description.
    
    Parameters:
    data (dict): A dictionary containing the attributes with id and codes.
    
    Returns:
    pd.DataFrame: Flattened DataFrame with columns 'id', 'code', and 'description'.
    """
    flattened_data = []

    for entry in data:
        for key, values in entry.items():
            attr_id = key
            for code_entry in values:
                flattened_data.append({
                    "id": attr_id,
                    "code": code_entry.get("code"),
                    "description": code_entry.get("description")
                })

    return pd.DataFrame(flattened_data)