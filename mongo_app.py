from datetime import datetime
from pymongo import MongoClient

col = MongoClient("mongodb://localhost:27017")["stockdb"]["prices"]
START = datetime(2011, 5, 2)          # same 10-year window as the Spark and MapReduce analyses


def show(rows):
    if not rows:
        print("  (no results)")
        return
    keys = list(rows[0].keys())
    print("  " + " | ".join(f"{k:>14}" for k in keys))
    for r in rows:
        print("  " + " | ".join(f"{str(r[k]):>14}" for k in keys))


def most_volatile():
    """Average daily (High-Low)/Close % per stock, 2011-05 onwards."""
    pipe = [
        {"$match": {"Date": {"$gte": START}, "Close": {"$gt": 0}}},
        {"$group": {"_id": "$Stock", "days": {"$sum": 1},
                    "avg_range": {"$avg": {"$multiply": [
                        {"$divide": [{"$subtract": ["$High", "$Low"]}, "$Close"]}, 100]}}}},
        {"$project": {"_id": 0, "Stock": "$_id", "days": 1,
                      "avg_range_pct": {"$round": ["$avg_range", 3]}}},
        {"$sort": {"avg_range_pct": -1}},
        {"$limit": 10},
    ]
    show(list(col.aggregate(pipe, allowDiskUse=True)))


def best_growth():
    """10-year CAGR from the first and last adjusted close in the window."""
    pipe = [
        {"$match": {"Date": {"$gte": START}}},
        {"$sort": {"Stock": 1, "Date": 1}},
        {"$group": {"_id": "$Stock", "first": {"$first": "$AdjClose"}, "last": {"$last": "$AdjClose"}}},
        {"$project": {"_id": 0, "Stock": "$_id",
                      "cagr_raw": {"$multiply": [{"$subtract": [{"$pow": [{"$divide": ["$last", "$first"]}, 0.1]}, 1]}, 100]}}},
        {"$sort": {"cagr_raw": -1}},
        {"$limit": 10},
        {"$project": {"Stock": 1, "cagr_pct": {"$round": ["$cagr_raw", 2]}}},
    ]
    show(list(col.aggregate(pipe, allowDiskUse=True)))

def yearly_summary(stock):
    pipe = [
        {"$match": {"Stock": stock}},
        {"$group": {"_id": {"$year": "$Date"}, "trading_days": {"$sum": 1},
                    "avg_close": {"$avg": "$AdjClose"}, "avg_volume": {"$avg": "$Volume"}}},
        {"$project": {"_id": 0, "year": "$_id", "trading_days": 1,
                      "avg_adj_close": {"$round": ["$avg_close", 1]},
                      "avg_volume": {"$round": ["$avg_volume", 0]}}},
        {"$sort": {"year": 1}},
    ]
    show(list(col.aggregate(pipe)))


def extreme_days(direction):
    pipe = [
        {"$match": {"Return": {"$ne": None}}},
        {"$sort": {"Return": direction}},
        {"$limit": 5},
        {"$project": {"_id": 0, "Stock": 1,
                      "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$Date"}},
                      "return_pct": {"$round": [{"$multiply": ["$Return", 100]}, 1]}}},
    ]
    show(list(col.aggregate(pipe, allowDiskUse=True)))


def price_history():
    stock = input("  Stock symbol (e.g. INFY): ").strip().upper()
    try:
        d1 = datetime.strptime(input("  From date (YYYY-MM-DD): ").strip(), "%Y-%m-%d")
        d2 = datetime.strptime(input("  To date   (YYYY-MM-DD): ").strip(), "%Y-%m-%d")
    except ValueError:
        print("  Please use the format YYYY-MM-DD.")
        return
    docs = list(col.find({"Stock": stock, "Date": {"$gte": d1, "$lte": d2}},
                         {"_id": 0, "Date": 1, "Close": 1, "AdjClose": 1, "Volume": 1}).sort("Date", 1))
    print(f"  {len(docs)} trading days found")
    for d in docs[:3] + ([] if len(docs) <= 6 else [None]) + docs[-3:]:
        print("   ...") if d is None else print("  ", d["Date"].date(), round(d["Close"], 2), round(d["AdjClose"], 2), d["Volume"])


MENU = """
===== NIFTY-50 MongoDB analyzer =====
 1. Most volatile stocks (avg daily range, 2011-05 on)
 2. Fastest-growing stocks (10-year CAGR)
 3. Yearly summary for one stock
 4. Biggest single-day gains
 5. Biggest single-day losses
 6. Price history between two dates
 0. Exit"""

while True:
    print(MENU)
    choice = input("Choose: ").strip()
    if choice == "1":
        most_volatile()
    elif choice == "2":
        best_growth()
    elif choice == "3":
        yearly_summary(input("  Stock symbol: ").strip().upper())
    elif choice == "4":
        extreme_days(-1)
    elif choice == "5":
        extreme_days(1)
    elif choice == "6":
        price_history()
    elif choice == "0":
        break
    else:
        print("  Invalid choice.")
