"""
Runs the shared benchmark queries against ClickHouse.
Runs from the host - ClickHouse's HTTP interface is exposed on localhost:8123.
"""
import json
import statistics
import time

import clickhouse_connect

from queries import QUERIES

RESULTS_PATH = "benchmark/results_clickhouse.json"
NUM_RUNS = 3


def main():
    client = clickhouse_connect.get_client(
        host="localhost", port=8123, username="default", password="clickhouse"
    )

    results = {"engine": "clickhouse", "queries": []}

    for q in QUERIES:
        durations = []
        result = None
        for _ in range(NUM_RUNS):
            start = time.perf_counter()
            result = client.query(q["clickhouse_sql"])
            durations.append(time.perf_counter() - start)

        entry = {
            "name": q["name"],
            "description": q["description"],
            "median_seconds": round(statistics.median(durations), 4),
            "all_runs_seconds": [round(d, 4) for d in durations],
            "row_count": len(result.result_rows),
        }
        if q["name"] == "category_time_aggregation":
            idx = result.column_names.index("num_titles")
            entry["total_num_titles"] = sum(row[idx] for row in result.result_rows)

        results["queries"].append(entry)
        print(f"[clickhouse] {q['name']}: median {statistics.median(durations):.4f}s over {NUM_RUNS} runs")

    with open(RESULTS_PATH, "w") as f:
        json.dump(results, f, indent=2)


if __name__ == "__main__":
    main()
