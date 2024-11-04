"""retrieve sdmx UNICEF data"""

import sdmx
import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

BASE_URL = "https://sdmx.data.unicef.org/"

__all__ = ["get_indicator", "get_sdmx_client", "get_dataflow_codebook"]


def get_sdmx_client() -> sdmx.Client:
    """
    Get sdmx client.

    Returns:
        sdmx.Client: The sdmx client instance.
    """
    client = sdmx.Client("UNICEF")
    return client


def list_dataflows(client: sdmx.Client) -> pd.DataFrame:
    """
    List dataflows.

    Args:
        client (sdmx.Client): The sdmx client instance.

    Returns:
        pd.DataFrame: A DataFrame with dataflow IDs and names.
    """
    flow_msg = client.dataflow()
    dataflows = sdmx.to_pandas(flow_msg.dataflow)
    df_dataflows = pd.DataFrame(dataflows).reset_index()
    df_dataflows.columns = ["id", "name"]
    return df_dataflows


def get_dataflow_codebook(
    client: sdmx.Client, dataflow_id: str
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Retrieve and organize dataflow dimensions and attributes into a codebook.

    Args:
        client (sdmx.Client): The sdmx client instance
        dataflow_id (str): The id of the dataflow to retrieve the codebook for

    Returns:
        tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: (df_indicators, df_dimensions, df_attributes) where
            - df_indicators is a DataFrame with the following columns:
                - indicator_code (str): The code for the indicator
                - indicator_name (str): The name of the indicator
            - df_dimensions is a DataFrame with the following columns:
                - dimension_value_code (str): The code for the dimension value
                - dimension_value_name (str): The name of the dimension value
                - dimension_code (str): The code for the dimension
                - dimension_name (str): The name of the dimension
            - df_attributes is a DataFrame with the following columns:
                - attribute_value_code (str): The code for the attribute value
                - attribute_value_name (str): The name of the attribute value
                - attribute_code (str): The code for the attribute
                - attribute_name (str): The name of the attribute
    """
    flow_msg = client.dataflow(dataflow_id)
    dataflow_structure = flow_msg.dataflow[dataflow_id].structure
    df_indicators = get_indicator_list(dataflow_structure)
    to_concat_dims = []
    to_concat_attrs = []
    for dimension in dataflow_structure.dimensions.components:
        if dimension.id == "INDICATOR":
            continue
        schema = dataflow_structure.dimensions.get(
            dimension
        ).local_representation.enumerated
        if not schema:
            logging.warning(
                f"Missing codebook for the dimension {dimension} in {dataflow_id}"
            )
            continue

        df_dimension_code = sdmx.to_pandas(schema).reset_index()

        if "parent" in df_dimension_code:
            df_dimension_code.drop(columns=["parent"], inplace=True)

        assert len(df_dimension_code.columns) == 2
        dimension_name = df_dimension_code.columns[1]
        df_dimension_code.columns = ["dimension_value_code", "dimension_value_name"]
        df_dimension_code["dimension_code"] = dimension.id
        df_dimension_code["dimension_name"] = dimension_name
        to_concat_dims.append(df_dimension_code)

    for attribute in dataflow_structure.attributes.components:
        try:
            schema = dataflow_structure.attributes.get(
                attribute
            ).local_representation.enumerated
            if not schema:
                logging.warning(
                    f"Missing codebook for the attribute {attribute} codebook in {dataflow_id}"
                )
                continue

            df_attribute_code = sdmx.to_pandas(schema).reset_index()

            if "parent" in df_attribute_code:
                df_attribute_code.drop(columns=["parent"], inplace=True)

            assert len(df_attribute_code.columns) == 2
            attribute_name = df_attribute_code.columns[1]
            df_attribute_code.columns = ["attribute_value_code", "attribute_value_name"]
            df_attribute_code["attribute_code"] = attribute.id
            df_attribute_code["attribute_name"] = attribute_name
            to_concat_attrs.append(df_attribute_code)
        except Exception as e:
            logging.warning(
                f"Missing codebook for the attribute {attribute} in {dataflow_id}: {e}",
            )

    df_dims = pd.concat(to_concat_dims, ignore_index=True)
    df_attrs = pd.concat(to_concat_attrs, ignore_index=True)
    return df_indicators, df_dims, df_attrs


def get_indicator_list(dataflow_structure) -> pd.DataFrame:
    """
    Get the list of indicators from the dataflow structure.

    Args:
        dataflow_structure: The dataflow structure

    Returns:
        pd.DataFrame: The DataFrame with the list of indicators
    """
    df_indicators = sdmx.to_pandas(
        dataflow_structure.dimensions.get("INDICATOR").local_representation.enumerated
    ).reset_index()
    if len(df_indicators.columns) == 3:
        df_indicators.columns = ["indicator_code", "indicator_name", "parent"]
    if len(df_indicators.columns) == 2:
        df_indicators.columns = [
            "indicator_code",
            "indicator_name",
        ]
    return df_indicators


def get_indicator(
    client: sdmx.Client,
    dataflow_id: str = None,
    indicator_id: str = None,
):
    """
    GET a single indicator from dataflow
    """
    dsd = client.dataflow(dataflow_id).structure[0]
    data = client.data(dataflow_id, key=dict(INDICATOR=indicator_id), dsd=dsd)
    data = data.data[0]
    df = sdmx.to_pandas(data)
    df = pd.DataFrame(df).reset_index()
    if df["value"].isna().all():
        raise ValueError("All values are None")
    return df
