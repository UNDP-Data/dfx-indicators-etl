"""utils for exploratory analysis of the transformed data"""

import pandas as pd

def print_essential_data_descriptives(df: pd.DataFrame):
    print(f"The dataframe has {df.shape[0]} rows and {df.shape[1]} columns")
    
    print("\n Names and data types for each column:")
    print(df.info())

    print("\n Unique value counts for each column:")
    print(df.nunique().sort_values(ascending=False))

    print("\n Missing value counts for each column:")
    print(df.isna().sum().sort_values(ascending=False))