"""
Runs the shared benchmark queries against raw Spark SQL over the Parquet Lake.
Must run inside the Spark cluster (via spark-submit) - see scripts/run_benchmark.sh.
"""
import json
import statistics
import time

from pyspark.sql import SparkSession

from queries import QUERIES

LAKE_PATH = "/opt/app/data/lake/titles"
RESULTS_PATH = "/opt/app/benchmark/results_spark.json"
NUM_RUNS = 3


def main():
    startup_start = time.perf_counter()
    spark = SparkSession.builder.appName("imdb-benchmark").getOrCreate()
    spark.read.parquet(LAKE_PATH).createOrReplaceTempView("titles")
    # Warm-up action: pays JVM/codegen/file-listing cost once, outside the
    # per-query timings below - mirrors a session that's already "up" by the
    # time an analyst starts querying it, same as ClickHouse's persistent daemon.
    spark.sql("SELECT count(*) FROM titles").collect()
    startup_seconds = time.perf_counter() - startup_start

    results = {"engine": "spark", "startup_seconds": round(startup_seconds, 3), "queries": []}

    for q in QUERIES:
        durations = []
        rows = None
        for _ in range(NUM_RUNS):
            start = time.perf_counter()
            rows = spark.sql(q["spark_sql"]).collect()  # .collect() forces execution - Spark is lazy otherwise
            durations.append(time.perf_counter() - start)

        entry = {
            "name": q["name"],
            "description": q["description"],
            "median_seconds": round(statistics.median(durations), 4),
            "all_runs_seconds": [round(d, 4) for d in durations],
            "row_count": len(rows),
        }
        # Correctness cross-check payload for one query, compared against
        # ClickHouse's result for the same query in compare.py.
        if q["name"] == "category_time_aggregation":
            entry["total_num_titles"] = sum(r["num_titles"] for r in rows)

        results["queries"].append(entry)
        print(f"[spark] {q['name']}: median {statistics.median(durations):.4f}s over {NUM_RUNS} runs")

    with open(RESULTS_PATH, "w") as f:
        json.dump(results, f, indent=2)

    spark.stop()


if __name__ == "__main__":
    main()
