# IMDb Lakehouse to OLAP Pipeline

A local pipeline that ingests a subset of the IMDb non-commercial datasets, cleans/transforms it with PySpark on a Dockerized Spark standalone cluster, writes it as partitioned Snappy Parquet (the "Lake"), loads it into ClickHouse (the "OLAP" serving layer), and benchmarks ClickHouse against raw Spark SQL on the same queries.

## Architecture

```
Kaggle (title.basics, title.ratings)          datasets.imdbws.com (title.episode)
              │                                            │
              └───────────────────┬────────────────────────┘
                                   ▼
                   PySpark ETL (etl_job.py)
             clean, cast types, left-join ratings +
             episode metadata onto one wide "titles" table
                                   ▼
        Partitioned Snappy Parquet - the "Lake"
        data/lake/titles/titleType=.../decade=.../*.parquet
                                   ▼
                  load_to_olap.py
     (ClickHouse's own file() table function reads the
      Parquet directly, server-side - no row-by-row Python)
                                   ▼
                ClickHouse `imdb.titles`
             (MergeTree, partitioned + ordered
              for sub-second analytical queries)
                                   ▼
              benchmark/ - same 4 queries run against
              raw Spark SQL vs. ClickHouse, timed and compared
```

Everything runs locally via Docker Compose: a Spark standalone cluster (one master, one worker) and a ClickHouse server.

## Setup & running it end to end

Prerequisites: Docker Desktop, Python 3.11 (`pip install clickhouse-connect`), a Kaggle account + API token (`~/.kaggle/kaggle.json`).

```bash
# 1. Bring up the cluster (Spark master/worker + ClickHouse; DDL auto-applies on first boot)
docker compose up -d

# 2. Download the source data
bash scripts/download_data.sh

# 3. Run the PySpark ETL - produces the partitioned Parquet Lake
bash scripts/run_etl.sh

# 4. Load the Lake into ClickHouse
python3.11 load_to_olap.py

# 5. Run the benchmark: Spark SQL vs ClickHouse, same queries
bash scripts/run_benchmark.sh
```

### A note on step 2 - the dataset doesn't quite match the assignment's link

The assignment points at Kaggle's `ashirwadsangwan/imdb-dataset`, but that mirror only bundles 5 tables (`name.basics`, `title.akas`, `title.basics`, `title.principals`, `title.ratings`) - it does **not** include `title.episode`, which the assignment explicitly asks for. `scripts/download_data.sh` fetches `title.basics`/`title.ratings` from Kaggle as instructed, and `title.episode.tsv.gz` directly from IMDb's own official non-commercial dataset distribution (`datasets.imdbws.com`) - same publisher, just the direct source, to fill the gap the Kaggle mirror leaves. `name.basics`, `title.akas`, and `title.principals` are intentionally not downloaded - out of scope for titles/ratings/episodes, and together an unnecessary ~8.4GB.

## Schema design

One denormalized, wide `titles` fact table - not three separate normalized tables. `title.episode` rows are really just the `tvEpisode`-typed rows of `title.basics` with extra parent/season/episode columns, so ratings and episode metadata are left-joined onto basics once, at ETL time. OLAP engines (and analysts) want fewer joins at query time, not more.

**Partitioning - satisfies both the "time-series" and "category-based" options, not just one:**
- **Parquet Lake:** `partitionBy(titleType, decade)` - `titleType` is low-cardinality (~11 values) and isolates episode data into its own slice; `startYear` is bucketed into `decade` rather than partitioned on the raw year, to avoid a 150+ folder explosion for a 12.6M-row dataset.
- **ClickHouse `imdb.titles`:** `PARTITION BY decade` (coarse physical split - whole decades pruned/dropped wholesale) + `ORDER BY (titleType, startYear, tconst)` (the physical sort order and sparse primary index within each partition - finer-grained than the partition key, and what most query filters actually hit).
- Secondary skip indexes: `minmax` on `averageRating`/`numVotes` (range filters), `bloom_filter` on `genres` (array membership filters).

**Sentinel values instead of NULL for `startYear`/`decade`:** ClickHouse doesn't allow nullable columns in a `PARTITION BY`/`ORDER BY` key. `0` (not a valid year) marks "unknown" in both the Parquet Lake's `decade` column and ClickHouse's `startYear`/`decade` columns - a deliberate choice, not an oversight (see the comments in `etl_job.py`, `ddl/01_titles.sql`, and `load_to_olap.py` for the full reasoning trail, including a genuinely blocking issue this avoided: ClickHouse's hive-partitioning reader infers one unified type per virtual partition column across an entire glob, and cannot mix a numeric decade folder with a literal `__HIVE_DEFAULT_PARTITION__` string folder in the same read).

## Performance Note - why ClickHouse

**OLAP vs. OLTP, in one sentence:** OLTP engines (Cassandra, MySQL - what I use daily at my current job) are row-stores optimized for many small point reads/writes; OLAP engines are column-stores optimized for scanning and aggregating over huge row counts, which is exactly this assignment's "sub-second on hundreds of millions of rows" requirement.

**Why not Redshift Spectrum**, despite it being what I already run in production (S3 lake + Spectrum external tables is literally my day job's architecture):
- It isn't local - no Docker image exists for it; a reviewer would need their own AWS Redshift cluster just to run this repo.
- It's a query-federation layer over S3 files, not a storage engine with its own optimized layout - no real indexes or primary keys on external tables, which would have left the DDL deliverable nearly empty.
- On a dataset this size, its fixed per-query network/cluster overhead could plausibly be slower than Spark itself, undermining the very thing this benchmark needs to prove.
- Reaching for the tool I already know wouldn't have demonstrated the judgment this exercise is actually testing.

**ClickHouse won on merit:** genuinely local (single Docker container), a real MergeTree storage engine with meaningful `ORDER BY`/`PARTITION BY`/skip-index design decisions, and (per the benchmark below) a large, easily-reproducible margin over raw Spark SQL on this exact dataset.

I also considered DuckDB (simpler, embedded, but a weaker "prove OLAP beats Spark" story since it isn't a persistent server) and Druid/Pinot (too much operational complexity to justify for a local single-node take-home).

### Benchmark results

Spark's one-time cluster startup/session overhead (JVM + `SparkSession` init, paid once): **9.57s** - reported separately since ClickHouse, as an always-on server, has no equivalent per-run cost. The table below is steady-state, per-query time (median of 3 runs each), which is the fair apples-to-apples comparison:

| Query | Spark (s) | ClickHouse (s) | Speedup |
|---|---:|---:|---:|
| Category x time aggregation (full scan) | 1.5299 | 0.0475 | **32.2x** |
| Top-rated popular series (filter+sort+limit) | 0.2015 | 0.0489 | **4.1x** |
| Top genres since 2010 (array explosion) | 0.9343 | 0.1025 | **9.1x** |
| Series with most episodes (group by) | 1.5118 | 0.0955 | **15.8x** |

**Correctness cross-check**, not just a speed claim: both engines' result sets for the category x time aggregation query were compared on total row count (`1,695,629`, matching `title.ratings`' row count exactly) - `MATCH`. A fast wrong answer proves nothing, so this is checked automatically in `benchmark/compare.py`, not just eyeballed.

The biggest win (32x) is the full-table aggregation - exactly where columnar storage, vectorized execution, and the sparse primary index matter most. The smallest (4.1x, still a clear win) is the filter+sort+limit query, where Spark's own Parquet partition pruning (confirmed separately to prune 99.24% of partitions on a filtered read) already captures some of the same benefit a partitioned external table would.

## Known local-only simplifications

Called out explicitly rather than left implicit:
- Spark/ClickHouse containers bind-mount `./data` from the host and run as `root` to avoid a uid mismatch against the official (non-Bitnami) Spark image's non-root default user. A real multi-tenant deployment would fix this with matching UIDs or named volumes instead.
- ClickHouse's `default` user password (`clickhouse`) is set in plaintext in `docker-compose.yml`, purely so the host-side Python client can authenticate over the exposed HTTP port. Fine for a local take-home; a real deployment would use secrets management.
- One Spark worker (2 cores), one ClickHouse node - not a true multi-node distributed benchmark. The comparison here is "purpose-built OLAP engine vs. general-purpose compute engine" on identical single-machine hardware, not "OLAP cluster vs. Spark cluster at production scale."

## Repo layout

```
docker-compose.yml       Spark master/worker + ClickHouse
etl_job.py               PySpark ETL: raw TSV -> partitioned Snappy Parquet Lake
load_to_olap.py          Loads the Parquet Lake into ClickHouse
ddl/01_titles.sql        ClickHouse MergeTree DDL (auto-applied on first container boot)
benchmark/               Shared queries + Spark/ClickHouse benchmark runners + comparison
scripts/                 download_data.sh, run_etl.sh, run_benchmark.sh
PROMPTS.md               Log of every prompt used to build this with Claude Code
```
