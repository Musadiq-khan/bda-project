# Stock Market Data Analysis Using Big Data Analytics (PJ26AI013)

BDA mini project and practicals on 21 years of NIFTY-50 daily prices (2000 to 2021).

## Mini project (PySpark)
Cleaning (split/bonus adjustment, missing-data handling), sector analysis,
correlations, a moving-average test, and a baseline ML model on a time-based split.
`load_data.py` -> `explore*.py`, `splits*.py` -> `build_clean.py` -> `analysis1.py`,
`sectors.py`, `correlation.py`, `moving_avg.py`, `ml_model.py`, `charts.py`

## Practical 1: MongoDB application
`load_mongo.py` (load 234,976 documents), `mongo_index_crud.py` (index comparison
and CRUD), `mongo_app.py` (menu-driven aggregation app), `compare_timing.py`.

## Practical 2: Hadoop
HDFS file management and a Python Hadoop Streaming job (average daily price
range per stock): see `mapreduce/`.

## Data
Download "NIFTY-50 Stock Market Data (2000 - 2021)" from Kaggle:
- the per-stock CSVs go in `data/stocks/`
- `NIFTY50_all.csv` and `stock_metadata.csv` go in `data/`

## Setup
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
Requires Java 17 (Spark), Hadoop 3.5.0 and MongoDB 8.0 for the practicals.
