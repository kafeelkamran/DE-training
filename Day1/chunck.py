import pandas as pd
import time
start = time.time()
reader = pd.read_csv('hw_200.csv',chunksize = 10000)

row_count = 0

for chunck in reader:
    row_count += len(chunck)

print("Row_count: ", row_count)
print("Chunked read time: ", time.time() - start)