import time
from datetime import datetime
from pymongo import MongoClient, ASCENDING

client = MongoClient("mongodb://localhost:27017")
db = client["stockdb"]
col = db["prices"]

def avg_ms(fn, repeat=20):
    t0 = time.perf_counter()
    for _ in range(repeat):
        out = fn()
    return (time.perf_counter() - t0) / repeat * 1000, out

def plan_summary(query):
    r = db.command("explain", {"find": "prices", "filter": query}, verbosity="executionStats")
    stats = r["executionStats"]
    stage = r["queryPlanner"]["winningPlan"]
    names = []
    while stage:
        names.append(stage["stage"])
        stage = stage.get("inputStage")
    return " -> ".join(names), stats["totalDocsExamined"], stats["nReturned"]

query = {"Stock": "TCS", "Date": {"$gte": datetime(2020, 1, 1), "$lt": datetime(2020, 4, 1)}}

# ---------- Part A: the effect of an index ----------
col.drop_indexes()                       # keep only the default _id index
ms, rows = avg_ms(lambda: list(col.find(query)))
plan, examined, returned = plan_summary(query)
print(f"WITHOUT index: {ms:.2f} ms/query | plan: {plan} | examined {examined} docs to return {returned}")

t0 = time.perf_counter()
col.create_index([("Stock", ASCENDING), ("Date", ASCENDING)], name="stock_date")
print(f"Index created in {(time.perf_counter() - t0) * 1000:.0f} ms")

ms2, rows = avg_ms(lambda: list(col.find(query)))
plan2, examined2, returned2 = plan_summary(query)
print(f"WITH index:    {ms2:.2f} ms/query | plan: {plan2} | examined {examined2} docs to return {returned2}")
print(f"Speed-up: {ms / ms2:.1f}x")

# ---------- Part B: CRUD ----------
print("\n--- CREATE ---")
col.insert_one({"Stock": "DEMO", "Date": datetime(2021, 5, 3), "Close": 100.0, "Volume": 5000})
res = col.insert_many([
    {"Stock": "DEMO", "Date": datetime(2021, 5, 4), "Close": 102.5, "Volume": 6000},
    {"Stock": "DEMO", "Date": datetime(2021, 5, 5), "Close": 99.0, "Volume": 4500},
])
print("Inserted DEMO documents:", col.count_documents({"Stock": "DEMO"}))

print("\n--- READ ---")
print("find_one:", col.find_one({"Stock": "DEMO"}, {"_id": 0}))
print("Last 3 trading days of INFY:")
for d in col.find({"Stock": "INFY"}, {"_id": 0, "Date": 1, "AdjClose": 1}).sort("Date", -1).limit(3):
    print("  ", d)
print("TCS days with volume above 50 million:", col.count_documents({"Stock": "TCS", "Volume": {"$gt": 50_000_000}}))

print("\n--- UPDATE ---")
col.update_one({"Stock": "DEMO", "Date": datetime(2021, 5, 4)}, {"$set": {"Close": 103.0}})
col.update_many({"Stock": "DEMO"}, {"$set": {"Note": "test data"}, "$inc": {"Volume": 100}})
for d in col.find({"Stock": "DEMO"}, {"_id": 0}).sort("Date", 1):
    print("  ", d)

print("\n--- DELETE ---")
col.delete_one({"Stock": "DEMO", "Date": datetime(2021, 5, 3)})
print("Deleted DEMO documents:", col.delete_many({"Stock": "DEMO"}).deleted_count, "more")
print("Documents left in collection (should be 234976):", col.count_documents({}))
