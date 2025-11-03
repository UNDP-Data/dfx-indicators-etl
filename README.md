# DFx ETL Pipeline

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/release/python-3120/)
[![License](https://img.shields.io/github/license/undp-data/dfx-etl-pipeline)](https://github.com/undp-data/dfx-etl-pipeline/blob/main/LICENSE)
[![Black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)
[![Conventional Commits](https://img.shields.io/badge/Conventional%20Commits-1.0.0-%23FE5196?logo=conventionalcommits&logoColor=white)](https://conventionalcommits.org)

A Python package containing an ETL pipeline for processing indicator data used by the [Data Futures Exchange](https://data.undp.org) (DFx).

> [!WARNING]  
> The package is currently undergoing a major revamp. Some features may be missing or not working as intended. Feel free to [open an issue](https://github.com/UNDP-Data/dfx-etl-pipeline/issues).

# Table of Contents
1. [Introduction](#introduction)
2. [Installation](#installation)
3. [Usage](#usage)
4. [Sources](#sources)
5. [Contributing](#contributing)
6. [License](#license)
7. [Contact](#contact)
---

## Introduction

The Data Futures Exchange (DFx) is an analytics platform designed to support data-driven decision-making at UNDP. This repository hosts a Python package developed to collect, process and consolidate indicator data from various sources for the use on the DFx. The project is structured as follows:

```bash
.
├── .github/                  # GitHub templates and settings
│   └── ...
├── src/                      # package source code
│   └── dfx_etl/
│       ├── data/             # auxiliary data shipped with the package
│       │   └── unsd-m49.csv  # https://unstats.un.org/unsd/methodology/m49
│       ├── pipelines/        # ETL pipelines for various sources
│       │   ├── __init__.py
│       │   ├── _base.py      # base classes to inherit from for new sources
│       │   ├── energydata_info.py 
│       │   ├── ...
│       │   └── world_bank.py # source-specific ETL steps
│       ├── storage/          # supported storage backends
│       │   ├── __init__.py
│       │   ├── _base.py      # base class to inherit from for new backends
│       │   ├── azure.py      # Azure Blob Storage backend
│       │   └── local.py      # local storage backend
│       ├── __init__.py
│       ├── utils.py          # utility functions
│       └── validation.py     # validation schemas and functions
├── .env.example              # .env files example
├── .gitattributes            # special git attributes
├── .gitignore                # untracked path patterns
├── CONTRIBUTING.md           # guidelines for contributors
├── LICENSE                   # license terms for the package
├── main.ipynb                # main notebook for execuring pipelines
├── Makefile                  # make commands for routine operations
├── pyproject.toml            # Python package metadata
├── README.md                 # this file
├── requirements_dev.txt      # development dependencies
└── requirements.txt          # core dependencies
```


## Installation

Currently, the package is distributed via GitHub only. You can install it with `pip`:

```bash
 pip install git+https://github.com/undp-data/dfx-etl-pipeline
```

See [VCS Support](https://pip.pypa.io/en/stable/topics/vcs-support/#vcs-support) for more details.

## Usage

The package provides ETL – or rather RTL – routines for a range of supported sources. Running a pipeline involves 3 steps:

1. **R**etriving raw data using a source-specific `Retriever`.
2. **T**ransforming raw data using a source-specific `Transformer`, which also validates the data.
3. **L**oading validated data to a supported storage backend.

These 3 steps are abstracted away in a single `Pipeline` class. The basic usage is demonstrated below.

```python
# import a supported backend and source
from dfx_etl.storage import AzureStorage
from dfx_etl.pipelines import who_int as source, Pipeline

# instantiate a pipeline with relevant metadata
pipeline = Pipeline(
    url="https://ghoapi.azureedge.net/api/",
    retriever=source.Retriever(),
    transformer=source.Transformer(),
    storage=storage,
)

# run the ETL steps one by one
pipeline.retrieve()
pipeline.transform()
pipeline.load()
# or just call the `pipeline` to run them all at once
pipeline()
```

For more details see [`main.ipynb`](main.ipynb).

> [!WARNING]  
> Running the pipeline requires the `.env` file at the root of the project. Depending on the storage backend, you might need to set different types of variables. See [`.env.example`](.env.example).


## Sources

Below is the list of sources currently supported by the package.

| **Name**         | **URL**                                                                                                          |
|------------------|------------------------------------------------------------------------------------------------------------------|
| Energydata.info  | [ELECCAP dataset](https://energydata.info/dataset/installed-electricity-capacity-by-country-area-mw-by-country)  |
| IHME             | [GBD 2021 dataset](https://ghdx.healthdata.org/gbd-2021)                                                         |
| ILO              | [ILOSTAT SDMX API](https://ilostat.ilo.org/resources/sdmx-tools/)                                                |
| IMF              | [IMF DataMapper API](https://www.imf.org/external/datamapper/api/help)                                           |
| SIPRI            | [SIPRI Milex dataset](https://www.sipri.org/databases/milex)                                                     |
| UNAIDS           | [UNAIDS Key Population Atlas](https://kpatlas.unaids.org)                                                        |
| UNICEF           | [UNICEF SDMX API](https://sdmx.data.unicef.org/overview.html)                                                    |
| UN Stats         | [UN Stats SDG API](https://unstats.un.org/sdgs/UNSDGAPIV5/swagger/index.html)                                    |
| WHO              | [WHO GHO API](https://www.who.int/data/gho/info/gho-odata-api)                                                   |
| World Bank       | [World Bank Indicator API](https://datahelpdesk.worldbank.org/knowledgebase/topics/125589-developer-information) |


## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md).

## License

This project is licensed under the GNU General Public License. See the [`LICENSE`](LICENSE) file.

## Contact

This project is part of [Data Futures Exchange (DFx)](https://data.undp.org) at UNDP. If you are facing any issues or would like to make some suggestions, feel free to [open an issue](https://github.com/undp-data/dfx-etl-pipeline/issues/new/choose). For enquiries about DFx, visit [Contact Us](https://data.undp.org/contact-us).
