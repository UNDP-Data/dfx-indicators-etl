## The Data Futures Platform Backend Pipeline - an ETL pipeline for the Data Futures Platform

### Table of Contents
1. [Introduction](#introduction)
3. [Usage](#usage)
4. [Contributing](#contributing)
5. [Sources](#sources)

### Introduction
The Data Futures Platform Backend Pipeline is an ETL pipeline that transforms data from various sources and translates it into a format that can be used by the [Data Futures Platform](data.undp.org). The pipeline is written in Python.
The list of sources that the pipeline currently supports can be found [here](#sources).

### Usage

The **Data Futures Platform Backend Pipeline** enables users to process and transform data from diverse sources into a standardized schema. Below are the key components and how to use them:

#### Jupyter Notebooks for Execution

The pipeline provides executable Jupyter notebooks stored in the `.notebooks` folder. These notebooks can be run using [papermill](https://papermill.readthedocs.io), which supports parameterized execution and automation. This setup simplifies batch processing and ensures consistency across runs.

#### Azure Integration

The pipeline supports seamless integration with Azure infrastructure for scalable execution:
- **Azure Data Factory (ADF)**: Incorporate the pipeline notebooks into ADF for orchestration and scheduling.
- **Azure Blob Storage**: Currently stores outputs and utility configuration files for the pipeline. Please see `.env.example`, you need to provide Azure Blob Container SAS token to interact with the Azure Blob Storage.

#### Source Modules and Common Schema

The `dfpp.sources` folder contains modules designed for individual data sources. Each module has the following components:
1. **Retrieval Submodule**: Responsible for fetching raw data from APIs or datasets provided by the supported sources.
2. **Transform Submodule**: Processes the raw data and maps it to a common schema format used by the platform.

This modular architecture allows for easy integration of new data sources and efficient updates to existing ones.

### Contributing
Follow these guidelines contribute:

#### Modifying an Existing Source
1. Locate the relevant module in the `.sources` directory.
2. Update the **retrieval submodule** to handle changes in the API endpoint, data format, or source structure.
3. Adjust the **transform submodule** to ensure the output conforms to the common schema format. In particular, check `dfpp.transformation.column_name_template.py` for the expected output schema after the transformations. Use `dfpp.transformation.value_handler.py` to coerce values to numeric.
4. Test the modifications using the Jupyter notebooks in the `.notebooks` folder.


#### Adding a new API source
0. Make sure the API source follows uniform schema and has a codebook allowing to consistently transform its series to comply with the target schema.
1. Create a new module in the `.sources` directory.
2. Recommend to get familiar with `transform.py` submodules for several sources to get an idea on the sequence of actions during the transformation and the implicit rules and assumptions.
3. Develop a **retrieval submodule** for fetching data from the new source's API or dataset.
4. Implement a **transform submodule** to standardize the data to the common schema format.
5. Develop a Jupyter notebook. Get familiar with the structure of the several source notebooks to get the idea which operations are offloaded to a source notebook vs a source module.

#### Ad-hoc indicators
**Indicators from one file per source or sources that do not support APIs are discouraged.**

In case one integrates **a single file from a single source** you may implement the transformation logics directly in a notebook `.notebooks` and do not develop a module within `dfpp.sources`. 

**An example**: 
`.notebooks/energydata_info.ipynb` - one requires to transform only one file per the whole source of origin, energydata.info.
`.notebooks/sipri_org.ipynb` - only one indicator from one an Excel file sheet is processed.

However if one requires to transform **multiple files/indicator series from one source** that does not support API calls, you can develop a module within `dfpp.sources` to handle that.

**An example:**
 `.notebooks/healthdata_org.ipynb` - one has to transform multiple series oncatenated into one file that follow the same schema. `dfpp.sources.healthdata_org` - contains `transform.py` submodule only since the source file will never be updated.


### Sources
# API Endpoints and Documentation

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
