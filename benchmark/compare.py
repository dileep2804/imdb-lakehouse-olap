"""
Reads both engines' benchmark results and prints a comparison table,
including a correctness cross-check (not just a speed claim - a fast wrong
answer proves nothing).
"""
import json

with open("benchmark/results_spark.json") as f:
    spark_results = json.load(f)
with open("benchmark/results_clickhouse.json") as f:
    ch_results = json.load(f)

spark_by_name = {q["name"]: q for q in spark_results["queries"]}
ch_by_name = {q["name"]: q for q in ch_results["queries"]}

print(f"Spark one-time cluster startup overhead: {spark_results['startup_seconds']}s")
print("(session init/warm-up - not counted in the per-query steady-state timings below)\n")

header = f"{'Query':<30} {'Spark (s)':>10} {'ClickHouse (s)':>15} {'Speedup':>10}"
print(header)
print("-" * len(header))

for name, spark_q in spark_by_name.items():
    ch_q = ch_by_name[name]
    spark_t = spark_q["median_seconds"]
    ch_t = ch_q["median_seconds"]
    speedup = spark_t / ch_t if ch_t > 0 else float("inf")
    print(f"{name:<30} {spark_t:>10.4f} {ch_t:>15.4f} {speedup:>9.1f}x")

print("\nCorrectness cross-check (category_time_aggregation total_num_titles):")
spark_total = spark_by_name["category_time_aggregation"]["total_num_titles"]
ch_total = ch_by_name["category_time_aggregation"]["total_num_titles"]
verdict = "MATCH" if spark_total == ch_total else "MISMATCH"
print(f"  Spark total: {spark_total}  |  ClickHouse total: {ch_total}  ->  {verdict}")
