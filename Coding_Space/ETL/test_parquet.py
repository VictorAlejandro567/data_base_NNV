import pandas as pd

df = pd.read_parquet("data/vehicle_type.parquet")
print(df.head())
print(df.columns)
print(df.dtypes)