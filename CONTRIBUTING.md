# Contributing to DFx ETL Pipeline

This document provides guidelines for contributors. Please take a moment to review this guide before contributing.

## Table of Contents

1. [Getting Started](#getting-started)
2. [How to Contribute](#how-to-contribute)
3. [Code Style](#code-style)
4. [Using Git](#using-git)
5. [New Sources](#new-sources)
6. [Reporting Issues](#reporting-issues)
7. [Pull Requests](#pull-requests)

## Getting Started

To contribute, you need to set up your local Python environment. Follow the steps below:

1. [Fork the repository](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/fork-a-repo).
2. [Clone your fork](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository):

   ```bash
   git clone https://github.com/<your-username>/dfx-etl-pipeline.git
   cd dfx-etl-pipeline
   ```

3. [Create and activate a virtual environment](https://docs.python.org/3.12/library/venv.html):

   ```bash
   python -m venv .venv
   source venv/bin/activate  # or venv\Scripts\activate on Windows
   ```

4. Install dependencies using `make`:

   ```bash
   make install
   ```

Once completed, you should be able to import `dfx_etl` package in your current virtual environment:

```python
import dfx_etl
print(dfx_etl.__version__)  # Output: '0.1.0' or similar
```

## How to Contribute

Here are some ways to contribute:

- Improve documentation
- Fix bugs
- Add support for new storage backends
- Implement support for new data sources
- Optimise the code for speed and clarity

## Code Style Guidelines

The codebase is formatted with `black` and `isort`. Use the provided [Makefile](Makefile) for these routine operations:

```shell
# lint and format
make lint
make format
```

## Using Git

Follow this branch naming convention:

- `feature/your-feature-name`
- `bugfix/fix-name`
- `doc/update-docs`

Create a new branch before making changes:

```bash
git checkout -b feature/your-feature-name
```

Commit messages must follow [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/), e.g.,

```bash
git add main.py
git commit -m
# Feat: add support for s3 storage backend
# Fix(validation): handle mixed data types in `value` column
# Docs: correct a few typos in `README.md`
```

## New Sources

Adding a new source to the pipeline requires going through several steps, which depend on the type of source in question. If the source does not have an API, you can download the files and upload them to a supported storage backend (local or Azure Blob Storage).

Running an ETL pipeline involving two key stages: retrieval and transformation. You will need to define the logic for both of these stages for any new source. To do so:

1. Create a new python module inside `pipelines` directory. Give it a descriptive name that refers to the domain, e.g., `undp_org.py`.
2. Inside the module, import the base classes you need to inherit from to define the ETL.
3. Define a `Retriever` class by inheriting from the `BaseRetriever`. This class defines the logic of how the data should be obtained.
4. Define a `Transformer` class by inheriting from the `BaseTransformer`. This class defines the logic of how the data should be transformed. The output of the transformer should be a standardised data frame that closely follows the schema defined in [`validation.py`](./src/dfx_etl/validation.py).

Here is a minimal example:

```python
# Steps 1: create dfx_etl/pipelines/undp_org.py

# add imports as needed
import pandas as pd
from pydantic import Field

# Step 2: import base classes
from ._base import BaseRetriever, BaseTransformer


# Step 3: define the retriever by inheriting from the base class
class Retriever(BaseRetriever):

    # redefine the location of your source file or API
    uri: HttpUrl = Field(
        default="https://hdr.undp.org/sites/default/files/2025_HDR/HDR25_Composite_indices_complete_time_series.csv",
        frozen=True,
        validate_default=True,
    )

    # redefine this method to implement source-specific logic
    def __call__(self, **kwargs) -> pd.DataFrame:
        df = pd.read_csv(str(self.uri), encoding="Windows-1252", **kwargs)
        return self._subset_hdi(df)

    #  add other methods as needed, but prefix them with an underscore
    def _subset_hdi(self, df: pd.DataFrame) -> pd.DataFrame:
        # subset only country code and HDI columns, e.g., 'hdi_1990', 'hdi_2020'.
        return df[["iso3"]].join(df.filter(regex=r"^hdi_\d{4}$", axis=1))


# Step 4: define the transformer by inheriting from the base class
class Transformer(BaseTransformer):

    # redefine this method to implement source-specific logic
    def __call__(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        df = df.melt(id_vars="iso3")
        df = df.join(df["variable"].str.split("_", expand=True))
        df["indicator_name"] = "Human Development Index"
        return self._arrange_columns(df)

    # same goes for other methods here
    def _arrange_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        columns = {
            "iso3": "country_code",
            0: "indicator_code",
            "indicator_name": "indicator_name",
            1: "year",
            "value": "value",
        }
        return df.reindex(columns=columns).rename(columns=columns)
```

For more details, see the implementations for other sources in `pipelines` submodule.

## Reporting Issues

If you find a bug or have a suggestion:

- Check if it’s already reported
- If not, [open a new issue](https://github.com/undp-data/dfx-etl-pipeline/issues/new)
- Fill out the selected template and submit it

## Pull Requests

Once you are done with the changes, you should:

1. Push your branch to your fork.
2. Open a pull request (PR) from your branch into the `main` branch.
3. Fill out the provided PR template.
4. Wait for feedback or approval.

---

Thank you for contributing!
