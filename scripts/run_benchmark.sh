#!/usr/bin/env bash
set -euo pipefail

echo "=== Running Spark SQL benchmark (inside the cluster) ==="
docker compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.executor.memory=2g \
  /opt/app/benchmark/run_spark_benchmark.py

echo
echo "=== Running ClickHouse benchmark (from host) ==="
python3.11 benchmark/run_clickhouse_benchmark.py

echo
echo "=== Comparison ==="
python3.11 benchmark/compare.py
