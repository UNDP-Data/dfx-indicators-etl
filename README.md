# dv-data-pipeline
A repo  for the DFP backend pipeline


## Azure Folder Structure
<img width="5151" alt="DFP Folder Structure" src="https://user-images.githubusercontent.com/35971997/234326548-3f38a2d8-6c4f-4a01-8600-d8360d3b967d.png">

## Old Transform Steps

1. Import necessary modules and utility functions.
3. Define the functions `move_files`, `to_millis`, `aggregate_operation` and `internal_aggregations` for later use in file handling, datetime conversions and data aggregations.
4. Read `region_countries_df` from an Excel sheet, rename columns and group by region.
5. Start PySpark job, importing necessary modules and initializing time, error counter, and data dictionaries.
6. Set `today` to the current date and list all base files from the specified location.
7. Read `source_df`, `source_meta_df`, `indicator_df` and `indicator_meta_df` from CSV files located in specified paths. Make `Source ID` and `Indicator ID` the index of these dataframes, respectively.
8. Back up the `indicator_meta_df`.
9. Initialize the `transform_time` to the current timestamp.
10. Iterate over each row in `indicator_df`:
    - If the `Source ID` from the row does not exist in `source_df`, skip the current iteration.
    - Retrieve `indicator_meta` and `source_meta` information based on `Indicator ID` and `Source ID`, respectively.
    - Update `last_indicator_time` and `last_source_time` according to the `Retrieved On` timestamps in the respective metadata.
    - If the data's `Frequency` is `Daily` or if `last_indicator_time` is 0 or less than `last_source_time`, carry out a transformation:
      - Set filename and determine which transformation notebook to use.
      - Populate `notebook_indicators` dictionary with the data's indicator information.
      - Update `indicator_meta_info` with the respective meta information.
11. For each notebook in `notebook_indicators`, attempt to run the notebook:
    - If the notebook runs successfully, run `internal_aggregations` for the indicators and update `indicator_meta_df`.
    - If an error occurs, log the error.
12. Write `indicator_meta_df` back to its original file path.
13. If there are any errors, read `error_log_df`, concatenate it with the new error count dataframe, and write it back to its original file path.

## Potential workflow change

```python-repl
def load_data(filename):
    # Load the data, handle file type (e.g. csv, excel)
    pass  
def handle_missing_values(df, columns):
    # Remove rows with missing values in specified columns
    pass  
def save_data(df, filename):
    # Save the transformed data
    pass  
def general_transform(df, transformation_type, indicator_mapping, base_filename, **kwargs):
    """
    A general data transformation function that supports different types of transformations.  
    Parameters:
    df: DataFrame, the source data to be transformed
    transformation_type: int, the type of transformation to apply (1, 2, or 3)
    indicator_mapping: dict, mapping from source indicator names to target indicator names
    base_filename: str, base filename for saving transformed data
    kwargs: various, additional arguments specific to different transformation types
    """  
	# Apply the appropriate transformation based on the type
    if transformation_type == 1:
            # Apply type 1 transformation
            transformed_df = type1_transform(df, indicator_mapping, base_filename, **kwargs)
    elif transformation_type == 2:
        # Apply type 2 transformation
        transformed_df = type2_transform(df, indicator_mapping, base_filename, **kwargs)
    elif transformation_type == 3:
        # Apply type 3 transformation
        transformed_df = type3_transform(df, indicator_mapping, base_filename, **kwargs)
    else:
        raise ValueError(f"Invalid transformation type: {transformation_type}")  
# Perform additional common transformations here, if any  
    return transformed_df  

def type1_transform(df, **kwargs):
    # Specific logic for type1 transformation
    pass  
def type2_transform(df, **kwargs):
    # Specific logic for type2 transformation
    pass  
def type3_transform(df, **kwargs):
    # Specific logic for type3 transformation
    pass
```

