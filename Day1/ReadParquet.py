import pandas as pd
df = pd.read_parquet('output.parquet')
# print(df.head(10))
print(df[10::])