"""
Loads the partitioned Parquet Lake into ClickHouse.

This does NOT read Parquet rows into Python and re-insert them one at a time -
that would pay Python object overhead plus a network round trip per row/batch.
Instead, Python submits a single INSERT INTO ... SELECT statement and lets
ClickHouse's own file() table function read the Parquet files server-side -
all the actual data movement happens inside ClickHouse's native engine.
"""
import clickhouse_connect

LAKE_GLOB = "lake/titles/**/*.parquet"

INSERT_SELECT_QUERY = f"""
INSERT INTO imdb.titles
SELECT
    tconst,
    primaryTitle,
    originalTitle,
    isAdult,
    ifNull(startYear, 0)             AS startYear,
    endYear,
    runtimeMinutes,
    genres,
    averageRating,
    numVotes,
    parentTconst,
    seasonNumber,
    episodeNumber,
    titleType,
    toInt16OrZero(toString(decade))  AS decade
FROM file('{LAKE_GLOB}', Parquet)
SETTINGS use_hive_partitioning = 1
"""


def main():
    client = clickhouse_connect.get_client(
        host="localhost", port=8123, username="default", password="clickhouse"
    )

    client.command("TRUNCATE TABLE imdb.titles")
    client.command(INSERT_SELECT_QUERY)

    row_count = client.command("SELECT count() FROM imdb.titles")
    print(f"Loaded {row_count} rows into imdb.titles")


if __name__ == "__main__":
    main()
