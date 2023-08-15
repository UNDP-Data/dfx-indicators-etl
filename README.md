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

## Transform Breakdown
1. There are two projects: AccessAllData and VaccineEquity. Each project has a folder in the `Projects` directory, named after the project


### The main transform notebook functions.
1. Loads the source files, and sets the index to the source_id column of the source data.
2. Triggers all the other transform notebooks, by supplying the necessary parameters. as indicators, that have the following format:
```json
[
  {
    "filename": source_filename,
    "indicator": indicator_id,
    "aggregate": aggregate_operation,
  },
  {
    "filename": source_filename_2,
    "indicator": indicator_id_2,
    "aggregate": aggregate_operation_2,
  }
]
```
3. 

### Common functionality for (most) transform notebooks, and suggested implementation.
1. In the source, if the country code and region code don't exist, there is need to add this information and name the files as needed. 
In the source configuration files add these necessary columns and before the saving of the files, carry out the addition of the country column if they don't exist.
If they do, carry out the renaming of the columns.
2. Indicator mapping variable: Maps the indicator id's to the column names in the source data. - In the indicator config - there needs to a config property that shows the column name for the indicator id.



### Summary of the proposed implementation
1. Add 1. `country_code_column` and `region_code_column` to the source configuration file to be able to add the country code and region code to the source data if they don't exist or rename the columns if they do exist.
2. Add `date_column` to the source configuration file to be able to rename and use it in a simple format in date column in the source data if it exists.
3. Save all the source data in one single format whenever possible. Instead of uploading as is, save the data in a single format. This will make it easier to read the data and carry out the necessary transformations.
4. In the old pipeline, they have implemented backups, and metadata recording for sources such as retrieval date, transform date etc, which needs to be updated in the new download.
5. The indicator config file should look as follows:
```ini
[Indicator]
indicator_id = testindicatorid
indicator_source_id = TEST_ID
indicator_display_name = Indicator display name
indicator_source_column_name = Indicator Column
indicator_source_column_data_type = float
transform_function = type1_transform
[metadata]
last_transform_date = 2021-01-01
```
5. The source config file should look as follows:

```ini
[source]
id = TEST_ID
name = World bank data
url = https://api.worldbank.org/v2/en/indicator/IQ.CPA.PUBS.XQ?downloadformat=csv
frequency = daily	
source_type = Auto
save_as = CPIA_PSMI.csv
retrieval_notebook = CPIA_retriever
download_format = excel
downloader_function = cpia_downloader
country_code_column = Country Code
region_code_column = Region Code
date_column = Date
[downloader_params]
file = API_IQ.CPA.PUBS.XQ_DS2_en_csv_v2_4538378.csv
[metadata]
last_download_date = 2021-01-01
```
6. The actual transformation could be as follows:
  - Load the indicator configuration file with storage module.
  - Since at this point, the source files are expected to be similar, in terms of the column names and common fields, no other operations need to be taken related by the transformation other than the actual transformation.
  - From the indicator configuration, decide the transformation function to use. based on the transformation function, load the necessary parameters.
  - Carry out the transformation and the generation of the transformed base csvs, and the output files aggregated as needed with regions, indicators and countries.
  - Write the metadata related to the transformation to the metadata section of the indicator config
  - In case of transformation errors, write the errors to the error log file.
  - create tasks for each indicator with the above implementation, and run the tasks in parallel with asyncio.


### Type transformation
The aim of the transform is to convert the source data into a format ready for publishing. The transform is expected to be a function that takes the source data as input and returns the transformed data as output.
1. Type 1 transformation: The year data is in columns, and can be based on multiple years. example `1992_employment`, `GDP_1990` or not based on any year eg `employment`, `GDP`
2. Type 2 transformation: This transformation is used when the source data is based on a single year
3. Type 3 transformation: This transformation is used when the source data is based on multiple years, and the years are in rows. example `1990`, `1991`, `1992` etc.

### Handling the errors
1. The errors could be stored in the `errors` folder in the project directory.
2. The errors could be stored in the following format:

[//]: # (download errors)
| SourceID | Error Date | Error Message |
| -------- |------------|---------------|
| TEST_ID  | 2023-05-22 | Error message |

[//]: # (transformation errors)
| SourceID | Error Date | Error Message |
| -------- |------------|---------------|
| TEST_ID  | 2023-05-22 | Error message |

Note: The error should be the full traceback text of the error. and logging should be done in the error log file.



