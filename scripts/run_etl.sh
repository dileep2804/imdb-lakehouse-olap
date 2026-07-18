#!/usr/bin/env bash
set -euo pipefail

# Runs the ETL driver against the Spark standalone cluster started by
# `docker compose up`. spark-submit executes *inside* the spark-master
# container, where /opt/app is the same project directory bind-mounted
# on the host (see docker-compose.yml).
docker compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.executor.memory=2g \
  /opt/app/etl_job.py
