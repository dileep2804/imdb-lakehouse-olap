"""
PySpark ETL job: IMDb Lakehouse.

Reads raw IMDb TSV/TSV.GZ source files, cleans/casts types, and denormalizes
ratings + episode metadata onto a single wide `titles` fact table. Writes the
result as Snappy-compressed Parquet, partitioned by titleType (category) and
decade (time-series) - the "Lake".
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, DoubleType, BooleanType
)

RAW_DATA_PATH = "/opt/app/data/raw"
LAKE_OUTPUT_PATH = "/opt/app/data/lake/titles"

# Explicit schemas: one read pass, deterministic types. inferSchema would cost
# a second full scan of these (multi-GB) files just to guess at types.
TITLE_BASICS_SCHEMA = StructType([
    StructField("tconst", StringType(), False),
    StructField("titleType", StringType(), True),
    StructField("primaryTitle", StringType(), True),
    StructField("originalTitle", StringType(), True),
    StructField("isAdult", StringType(), True),        # "0"/"1" -> bool below
    StructField("startYear", StringType(), True),       # "YYYY" -> int below
    StructField("endYear", StringType(), True),
    StructField("runtimeMinutes", StringType(), True),
    StructField("genres", StringType(), True),          # comma-separated -> array below
])

TITLE_RATINGS_SCHEMA = StructType([
    StructField("tconst", StringType(), False),
    StructField("averageRating", DoubleType(), True),
    StructField("numVotes", LongType(), True),
])

TITLE_EPISODE_SCHEMA = StructType([
    StructField("tconst", StringType(), False),
    StructField("parentTconst", StringType(), True),
    StructField("seasonNumber", StringType(), True),
    StructField("episodeNumber", StringType(), True),
])


def read_tsv(spark, path, schema):
    """IMDb non-commercial datasets use tab separators and '\\N' as the null marker."""
    return (
        spark.read
        .option("sep", "\t")
        .option("header", True)
        .option("nullValue", "\\N")
        .schema(schema)
        .csv(path)  # .gz sources are decompressed transparently based on file extension
    )


def build_titles_lake(spark):
    basics = read_tsv(spark, f"{RAW_DATA_PATH}/title.basics.tsv", TITLE_BASICS_SCHEMA)
    ratings = read_tsv(spark, f"{RAW_DATA_PATH}/title.ratings.tsv", TITLE_RATINGS_SCHEMA)
    episodes = read_tsv(spark, f"{RAW_DATA_PATH}/title.episode.tsv.gz", TITLE_EPISODE_SCHEMA)

    basics_clean = (
        basics
        .withColumn("isAdult", F.col("isAdult").cast(BooleanType()))
        .withColumn("startYear", F.col("startYear").cast(IntegerType()))
        .withColumn("endYear", F.col("endYear").cast(IntegerType()))
        .withColumn("runtimeMinutes", F.col("runtimeMinutes").cast(IntegerType()))
        .withColumn("genres", F.split(F.col("genres"), ","))
        .withColumn("decade", F.coalesce((F.floor(F.col("startYear") / 10) * 10).cast(IntegerType()), F.lit(0))
        )
    )

    episodes_clean = (
        episodes
        .withColumn("seasonNumber", F.col("seasonNumber").cast(IntegerType()))
        .withColumn("episodeNumber", F.col("episodeNumber").cast(IntegerType()))
    )

    return (
        basics_clean
        .join(ratings, on="tconst", how="left")
        .join(episodes_clean, on="tconst", how="left")
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-path", default=LAKE_OUTPUT_PATH)
    parser.add_argument("--num-partitions", type=int, default=16)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("imdb-lakehouse-etl").getOrCreate()

    titles = build_titles_lake(spark)

    (
        titles
        .repartition(args.num_partitions, "titleType", "decade")
        .write
        .mode("overwrite")
        .partitionBy("titleType", "decade")
        .option("compression", "snappy")
        .parquet(args.output_path)
    )

    spark.stop()


if __name__ == "__main__":
    main()
