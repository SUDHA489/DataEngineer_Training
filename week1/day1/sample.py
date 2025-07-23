import time
import pandas as pd

start=time.time()
reader=pd.read_csv("https://people.sc.fsu.edu/~jburkardt/data/csv/hw_200.csv",chunksize=10000)
rc=0
for chunk in reader:
    rc+=len(chunk)

print("Row Count:",rc)
print("chunked read time:",time.time()-start)