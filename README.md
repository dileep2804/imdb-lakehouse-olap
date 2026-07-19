# IMDb Lakehouse to OLAP Pipeline

PySpark (on a Dockerized Spark standalone cluster) cleans the IMDb non-commercial datasets and writes partitioned Snappy Parquet (the "Lake"); ClickHouse serves the analytics.

## Setup

Prerequisites: Docker Desktop, Python 3.11 (`pip install -r requirements.txt`), a Kaggle API token (`~/.kaggle/kaggle.json`).

```bash
docker compose up -d                 # Spark master/worker + ClickHouse (DDL auto-applies)
bash scripts/download_data.sh        # title.basics, title.ratings, title.episode
bash scripts/run_etl.sh              # PySpark ETL -> partitioned Parquet Lake
python3.11 load_to_olap.py           # load the Lake into ClickHouse
bash scripts/run_benchmark.sh        # Spark SQL vs ClickHouse, same queries
```

## Performance Note - why ClickHouse

OLAP engines are column-stores built for scanning/aggregating over huge row counts - what this assignment's "sub-second on hundreds of millions of rows" actually calls for, versus the row-store OLTP engines (Cassandra, MySQL) I use daily in production.

I deliberately did **not** reach for Redshift Spectrum, despite it being my day-to-day production architecture (S3 lake + Spectrum external tables): it isn't local (no Docker image exists for it - a reviewer would need their own AWS cluster), it's a query-federation layer with no real indexes on external tables (would've left the DDL deliverable empty), and its fixed per-query network overhead could plausibly lose to Spark on a dataset this small - the opposite of what this benchmark needs to prove. I also considered DuckDB (simpler, but not a persistent server, weaker "prove OLAP beats Spark" story) and Druid/Pinot (too much operational complexity for a local single-node take-home).

**ClickHouse won on merit:** genuinely local, a real MergeTree engine with meaningful `ORDER BY`/`PARTITION BY` design, and a large, reproducible margin over raw Spark SQL below.

### Benchmark results

Spark's one-time cluster/session startup (~8.6s) is excluded from the per-query numbers below (median of 3 runs each) since ClickHouse, as an always-on server, has no equivalent cost:

| Query | Spark (s) | ClickHouse (s) | Speedup |
|---|---:|---:|---:|
| Category x time aggregation (full scan) | 1.0100 | 0.0331 | **30.5x** |
| Top-rated popular series (filter+sort+limit) | 0.2224 | 0.0160 | **13.9x** |
| Top genres since 2010 (array explosion) | 0.9469 | 0.1111 | **8.5x** |
| Series with most episodes (group by) | 1.2104 | 0.1119 | **10.8x** |

Both engines' results for the aggregation query were also cross-checked for correctness (`1,695,629` rows, matching exactly) - `benchmark/compare.py`.
