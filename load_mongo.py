import time
import pandas as pd
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017")
col = client["stockdb"]["prices"]

# 1) Read the cleaned Parquet data and tidy it for MongoDB
df = pd.read_parquet("data/clean_stocks.parquet")
df["Stock"] = df["Stock"].astype(str)
df["Date"] = pd.to_datetime(df["Date"])
df = (df[["Stock", "Date", "Open", "High", "Low", "Close", "Volume", "AdjClose", "ret"]]
        .rename(columns={"ret": "Return"})
        .sort_values(["Stock", "Date"]))

# 2) Turn rows into dictionaries; missing values become None (null in MongoDB)
obj = df.astype(object)
obj = obj.where(df.notna(), None)
records = obj.to_dict("records")

# 3) Insert in batches and time it
col.drop()
t0 = time.time()
for i in range(0, len(records), 10000):
    col.insert_many(records[i:i + 10000], ordered=False)
print(f"Inserted {len(records)} documents in {time.time() - t0:.1f} s")

# 4) Verify
print("Documents in collection:", col.count_documents({}))
print("Sample document:", col.find_one({"Stock": "TCS"}))
stats = client["stockdb"].command("dbstats")
print(f"Data size: {stats['dataSize'] / 1e6:.1f} MB | storage on disk: {stats['storageSize'] / 1e6:.1f} MB")
