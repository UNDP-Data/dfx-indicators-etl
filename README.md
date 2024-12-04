## The Data Futures Platform Backend Pipeline - an ETL pipeline for the Data Futures Platform

### Table of Contents
1. [Introduction](#introduction)
2. [Usage](#usage)
3. [Contributing](#contributing)
4. [Sources](#sources)
---
### Introduction

The Data Futures Platform Backend Pipeline is an ETL pipeline that transforms data from various sources and translates it into a format that can be used by the [Data Futures Platform](data.undp.org). The pipeline is written in Python.
The list of sources that the pipeline currently supports can be found [here](#sources).

---
### Usage

The **Data Futures Platform Backend Pipeline** enables users to process and transform data from diverse sources into a standardized schema. Below are the key components and how to use them:

#### Jupyter Notebooks for Execution

The pipeline provides executable Jupyter notebooks stored in the [.notebooks](./notebooks) folder. These notebooks can be run using [papermill](https://papermill.readthedocs.io), which supports parameterized execution and automation. This setup simplifies batch processing and ensures consistency across runs.

#### Azure Integration

The pipeline supports seamless integration with Azure infrastructure for scalable execution:
- **Azure Data Factory (ADF)**: Incorporate the pipeline notebooks into ADF for orchestration and scheduling.
- **Azure Blob Storage**: Currently stores outputs and utility configuration files for the pipeline. Please see [`.env.example`](./.env.example), you need to provide Azure Blob Container SAS token to interact with the Azure Blob Storage.
  
---

#### General Workflow Overview

The data is extracted from various APIs (see [sources](#sources)) or, in rare cases, from ad-hoc sources. The ETL process operates within a Jupyter notebook located in the [`.notebooks`](./notebooks/) folder for a specific source, utilizing the [dfpp.sources](./dfpp/sources/) module to facilitate the ETL. While the notebooks enable execution, the modules house the scripts that govern data retrieval and transformation.

The ETL pipeline for each source extracts data from a supported list of external APIs and transforms it into a [tidy data format](https://cran.r-project.org/web/packages/tidyr/vignettes/tidy-data.html).

> - Each variable is a column; each column is a variable.  
> - Each observation is a row; each row is an observation.  
> - Each value is a cell; each cell is a single value.

Schema column names and variable naming conventions are defined in [dfpp.transformation.column_name_template.py](./dfpp/transformation/column_name_template.py). 

**Key Highlights**:
- Indicator series dimensions must be prefixed with `DIMENSION_COLUMN_PREFIX = "disagr_"`.
- Series observation properties must be prefixed with `SERIES_PROPERTY_PREFIX = "prop_"`.
- `CANONICAL_COLUMN_NAMES` must be present in the set; if missing, they are generated and set to `None`.

| source                | series_id       | series_name                             | alpha_3_code | year | disagr_region | disagr_area | disagr_gender | prop_unit | prop_observation_type | value | prop_value_label |
|-----------------------|-----------------|-----------------------------------------|--------------|------|---------------|-------------|---------------|-----------|------------------------|-------|------------------|
| https://example.com   | SERIES_ID_CODE | Safely Managed Drinking Water Services | IND          | 2020 | Asia          | Urban       | Female        | Percent   | Estimated Value       | 88.2  |                  |

---

#### ETL Workflow Pattern Overview Illustrated Using `unstats_un_org` example

1. **Run the ETL Notebook**: Execute the code in the Jupyter notebook [notebooks.unstats_un_org.ipynb](./notebooks/unstats_un_org.ipynb).

2. **Retrieve Indicator Metadata**:  
   Use the [sources.unstats_un_org.retrieve](./dfpp/sources/unstats_un_org/retrieve.py) submodule methods to fetch a list of available indicator series. At this stage, it is also typically possible to retrieve a list of available observation dimensions/disaggregations (e.g., age, sex, locality). Additionally, consult the API’s documentation to access a codebook that defines the available data.

3. **Fetch Indicator Data**:  
   Retrieve the raw indicator data from the API.

4. **Transform Data into Tidy Format**:  
   Using the codebook schema, transform the data into the [tidy data format](https://cran.r-project.org/web/packages/tidyr/vignettes/tidy-data.html). Key transformation steps include:
   - Rename observation dimension and property variables to adhere to prefix conventions.  
   - Ensure that dimension and property values are human-readable; use codebook to facilitate the code to human-readable value replacement.
   - Convert country codes to alphabetic ISO3 format.
   - Coerce `value` to numeric, setting invalid values to `None` using the [transformation.value_handler](./dfpp/transformation/value_handler.py) module. This module emulates the [Pandas `coerce on error` policy](https://pandas.pydata.org/docs/reference/api/pandas.to_numeric.html) but attempts additional cleaning before conversion.  
   - Rename remaining columns to match canonical column names.  
   - Fill missing canonical column names with `None`.  
   - Drop non-country observations (e.g., regions).  
   - Drop non-annual observations (e.g., quarter, monthly).  
   - Validate that the series contains observations after transformation.  
   - Ensure no dimension is missing or redundant by validating for duplicates after dropping observation `value`.
> [!TIP]
>  See [transformation.value_handler](./dfpp/transformation/) for modules that help with handling canonical values, alphabetic iso3 country codes. Also this module depends on [country-converter](https://pypi.org/project/country-converter/1.3/) module for some country code transformations.

5. **Publish the Transformed Data**:  
   Save the transformed data as a Parquet file and publish it to an Azure Blob Storage container using the [dfpp.publishing](./dfpp/publishing) module.
---
### Contributing
Follow these guidelines contribute:

#### Source Modules and Common Schema

The [dfpp.sources](./dfpp/sources) folder contains modules designed for individual data sources. Each module has the following components:
1. **Retrieval Submodule**: Responsible for fetching raw data from APIs or datasets provided by the supported sources.
2. **Transform Submodule**: Processes the raw data and maps it to a common schema format used by the platform.

This modular architecture allows for easy integration of new data sources and efficient updates to existing ones.

#### Modifying an Existing Source
1. Locate the relevant module in the [dfpp.sources](./dfpp/sources) directory.
2. Update the **retrieval submodule** to handle changes in the API endpoint, data format, or source structure.
3. Adjust the **transform submodule** to ensure the output conforms to the common schema format.
> [!TIP]
>  In particular, check [dfpp.transformation](./dfpp/transformation/) for the expected output schema after the transformations.
5. Test the modifications using the Jupyter notebooks in the [.notebooks](./notebooks) folder.

#### Adding a new API source
0. Make sure the API source follows uniform schema and has a codebook allowing to consistently transform its series to comply with the target schema.
1. Create a new module in the [dfpp.sources](./dfpp/sources) directory.
2. Recommend to get familiar with `transform.py` submodules for several sources to get an idea on the sequence of actions during the transformation and the implicit rules and assumptions.
3. Develop a **retrieval submodule** for fetching data from the new source's API or dataset.
4. Implement a **transform submodule** to standardize the data to the common schema format.
5. Develop a Jupyter notebook. Get familiar with the structure of the several source notebooks to get the idea which operations are offloaded to a source notebook vs a source module.

#### Ad-hoc indicators
> [!CAUTION]
> **Indicators from one file per source or sources that do not support APIs are discouraged due to poor maintainability.**
> Check if there are any similar or equivalent indicators that are available for API sources prior to proceeding with ad-hoc indicator integration requests.

In case one integrates **a single file from a single source** you may implement the transformation logics directly in a notebook and do not develop a module within [dfpp.sources](./dfpp/sources). 

**An example**: 
[energydata.info](./notebooks/energydata_info.ipynb) - one requires to transform only one file per the whole source of origin.

[sipri.org](./notebooks/sipri_org.ipynb) - only one indicator from one an Excel file sheet is processed.

However if one requires to transform **multiple files/indicator series from one source** that does not support API calls, you can develop a module within [dfpp.sources](./dfpp/sources) to handle that.

**An example:**
 [healthdata.org](.notebooks/healthdata_org.ipynb) - one has to transform multiple series concatenated into one file that follow the same schema. Thus, it contains `transform.py` submodule only since the source file will never be updated.

---
### Sources
# Supported Sources and Documentation

| **Source**       | **API Endpoint Base URL**                                         | **Documentation Page**                                                        |
|------------------|------------------------------------------------------------------|-------------------------------------------------------------------------------|
| **ILO**          | [https://rplumber.ilo.org/](https://rplumber.ilo.org/)             | [ilostat web service](https://rplumber.ilo.org/__docs__/)                      |
|  **ILO(bulk file download)**         | [Bulk file download endpoint](https://webapps.ilo.org/ilostat-files/WEB_bulk_download/html/bulk_indicator.html) | [Bulk download guide](https://webapps.ilo.org/ilostat-files/WEB_bulk_download/html/bulk_main.html)                      |
| **UN SDG API**   | [https://unstats.un.org/sdgapi/swagger#/](https://unstats.un.org/sdgapi/swagger#/)| [UN SDG API Documentation](https://unstats.un.org/sdgapi/swagger#/)| 
| **WHO GHO API**  | [https://www.who.int/data/gho/info/gho-odata-api](https://www.who.int/data/gho/info/gho-odata-api) | [WHO GHO API Documentation](https://www.who.int/data/gho/info/gho-odata-api) |
| **UNICEF**       | [https://sdmx.data.unicef.org/](https://sdmx.data.unicef.org/)     | [UNICEF Data](https://sdmx.data.unicef.org/)                                    |
| **IMF**          | [https://www.imf.org/external/datamapper/api/](https://www.imf.org/external/datamapper/api/) | [IMF Data API Documentation](https://www.imf.org/external/datamapper/api/help)|
| **World Bank**   | [http://api.worldbank.org/v2/](http://api.worldbank.org/v2/)       | [World Bank API Documentation](https://data.worldbank.org/developers)          |
| **SIPRI**        | Not specified, Individual files transformed                       | [SIPRI Data](https://sipri.org/databases)                                      |
| **HealthData.org**| Not specified, Individual files transformed                      | [HealthData.org](https://www.healthdata.org/)                                  |
| **EnergyData.info** | Not specified, Individual files transformed                    | [EnergyData.info](https://energydata.info/)                                    |

