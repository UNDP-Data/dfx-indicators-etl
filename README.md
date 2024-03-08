## The Data Futures Platform Backend Pipeline - an ETL pipeline for the Data Futures Platform

### Table of Contents
1. [Introduction](#introduction)
2. [Installation](#installation)
3. [Usage](#usage)
4. [Contributing](#contributing)
5. [License](#license)

### Introduction
The Data Futures Platform Backend Pipeline is an ETL pipeline that transforms data from various sources and translates it into a format that can be used by the [Data Futures Platform](data.undp.org). The pipeline is written in Python.
The list of sources that the pipeline currently supports can be found [here](#sources).

### Installation
The pipeline requires Python 3.10 or higher. To install the pipeline as a Python package, run the following command:
```bash
pip install git+https://github.com/UNDP-Data/dv-data-pipeline.git
```

### Usage
There are several components to the pipeline, each of which can be run independently. The pipeline can be run in its entirety by running the following command:
```bash
python3 -m dfpp.cli run
```

Before running the pipeline, you need to set the following environment variables:
```bash
AZURE_STORAGE_CONNECTION_STRING=<connection string for Azure storage account>
AZURE_STORAGE_CONTAINER_NAME=<name of Azure storage container>
ROOT_FOLDER=<Root folder of the storage>
``` 
```Usage: python dfpp.cli [OPTIONS]
-h, --help                           Show this message and exit.
-l log-level, --log-level log-level  Set the log levell. {INDO, DEBUG, TRACE}
-i --indicators                      Run the indicator pipeline.
-f --filter                          Filter the indicators to run using specific phrases/substrings etc.
-e, --load-env-file                  Load the environment variables from the .env file.


Commands:
  list
  run

Stage 1: Downloading data
Usage: python3 -m dfpp.cli -e run download [OPTIONS]

Options:
  -s, --indicators [<indicator>]  Run the download pipeline for a specific indicator. It is going to download the sources for this indicator.
  -h, --help                   Show this message and exit.
  
Stage 2: Transforming data
Usage: python3 -m dfpp.cli -e run transform [OPTIONS]

Options:
  -i, --indicators [<indicator>]  Run the transform pipeline for a specific indicator. It is going to transform the sources for this indicator.
  -h, --help                      Show this message and exit.
  
Stage 3: Publishing data
Usage: python3 -m dfpp.cli -e run publish [OPTIONS]

Options:
  -i, --indicators [<indicator>]  Run the publish section of the pipeline for a specific indicator. It is going to upload data sources for this indicator.
  -h, --help                      Show this message and exit.
  

All stages:
Usage: python3 -m dfpp.cli -e run [OPTIONS]

Options:
  -i, --indicators [<indicator>]  Run the pipeline for a specific indicator(s). It is going to download, transform and upload the sources for the specified indicator(s). If
                                  no indicator is specified, it is going to run the pipeline for all indicators.
  -h, --help                      Show this message and exit.
```

#### Indicators
The pipeline is designed to be run for indicators. This means that the starting point of the pipeline is indicator based.
The list of indicators that the pipeline currently supports can be found [here](indicators.md).

##### Adding a new indicator
To add a new indicator to the pipeline, you need to create a new file in the `config/indicators` directory. The name of the file should be the name of the intended `indicator id`. Make sure that the filename is unique and that there is no indicator id similar to it
in the `config/indicators` directory. The file should be a `cfg or ini` file and should contain the following fields:
```ini
[indicator]
indicator_id = <indicator id>
indicator_name = <indicator name>
display_name = <display name>
source_id = <source id>
data_type = float
frequency = <frequency of updates>
aggregatetype = <aggregation type>
preprocessing = <preprocessing function>
transform_function = <transform function>
group_name = <group name>
value_column = <value column>
year = <year column>
column_substring = <column substring>
sheet_name = <sheet name>
[metadata]
initial_download_date = <initial download date>
last_download_date = <last download date>
last_transform_date = <last transform date>
last_upload_date = <last upload date>
```

The `indicator` section contains the following fields:
- `indicator_id`: The id of the indicator. This is the id that will be used to identify the indicator in the pipeline.
- `indicator_name`: The name of the indicator. This is the name that will be used to identify the indicator in the pipeline.
- `display_name`: The display name of the indicator. This is the name that will be used to display the indicator in the Data Futures Platform.
- `source_id`: The id of the source. This is the id that will be used to identify the source in the pipeline. The source id should be unique. 
- `data_type`: The data type of the indicator. This can be either `float`,`int`,`boolean` or `string` depending on the data type of the column(s) indicator in the source file.
- `frequency`: The frequency of updates for the indicator.
- `aggregatetype`: The aggregation type of the indicator. This can be either `sum`,`mean`,`median`,`mode`,`min`,`max`,`first`,`last` or `count` depending on the aggregation type of the indicator in the source file.
- `preprocessing`: The preprocessing function of the indicator. As the name suggests, this function is used to preprocess the data before it is transformed. This function should be defined in the `dfpp.preprocessing` module.
- `transform_function`: The transform function of the indicator. This function is used to transform the data into a format that can be used by the Data Futures Platform. This function should be defined in the `dfpp.transform_functions` module.
- `group_name`: The group name of the indicator.
- `value_column`: The name of the column that contains the values of the indicator.
- `year`: The year for which the indicator is calculated. Note that if the indicator is calculated for multiple years, the year column value should be `None`.
- `column_substring`: The substring that is used to identify the column(s) that contain the values of the years for the indicator.
- `sheet_name`: The name of the sheet that contains the data for the indicator.
- `metadata`: The metadata section contains the following fields:
    - `initial_download_date`: The date on which the indicator was first downloaded.
    - `last_download_date`: The date on which the indicator was last downloaded.
    - `last_transform_date`: The date on which the indicator was last transformed.
    - `last_upload_date`: The date on which the indicator was last uploaded.

##### Adding a new source
Once the indicator has been added, you need to add the source for the indicator. The source should be added to the `config/sources/<source_id>/<source_id>.cfg` directory. The name of the file should be the name of the intended `source id`. Make sure that the filename is unique and that there is no source id similar to it.
The file should be a `cfg or ini` file and should contain the following fields:
```ini
[source]
id = <source id>
name = <source name>
url = <source url>
frequency = <frequency of updates>
source_type = <source type>
save_as = <save as>
file_format = <file format>
downloader_function = <downloader function>
country_iso3_column = <country iso3 column>
country_name_column = <country name column>
datetime_column = <datetime column>
year = <year column>
group_column = <group column>
country_code_aggregate = <country code aggregate>
aggregate = <aggregate>

[downloader_params]
```

The `source` section contains the following fields:
- `id`: The id of the source. This is the id that will be used to identify the source in the pipeline. The source id should be unique.
- `name`: The name of the source. This is the name that will be used to identify the source in the pipeline.
- `url`: The url of the source.
- `frequency`: The frequency of updates for the source.
- `source_type`: The type of the source. This can be either `csv`,`excel`,`json` or `xml` depending on the type of the source.
- `save_as`: The name of the file in which the source will be saved.
- `file_format`: The format of the file in which the source will be saved. This can be either `csv`,`excel`,`json` or `xml` depending on the format of the file in which the source will be saved.
- `downloader_function`: The downloader function of the source. This function is used to download the source. This function should be defined in the `dfpp.downloader_functions` module.
- `country_iso3_column`: The name of the column that contains the iso3 codes of the countries.
- `country_name_column`: The name of the column that contains the names of the countries.
- `datetime_column`: The name of the column that contains the datetime values.
- `year`: The name of the column that contains the year values.
- `group_column`: The name of the column that contains the group values.
- `country_code_aggregate`: The country code aggregate of the source. This can be either `sum`,`mean`,`median`,`mode`,`min`,`max`,`first`,`last` or `count` depending on the country code aggregate of the source.
- `aggregate`: The aggregate of the source. This can be either `sum`,`mean`,`median`,`mode`,`min`,`max`,`first`,`last` or `count` depending on the aggregate of the source.
- `downloader_params`: This section contains the parameters that are required by the downloader function of the source.


