"""
Single source of truth for the benchmark queries, so the Spark SQL and
ClickHouse SQL versions of "the same question" can't silently drift apart.
Only genre-explosion syntax differs (explode() vs ARRAY JOIN) - everything
else is deliberately near-identical ANSI SQL.
"""

QUERIES = [
    {
        "name": "category_time_aggregation",
        "description": "Title count + avg rating by titleType and decade (full-table scan)",
        "spark_sql": """
            SELECT titleType, decade, count(*) AS num_titles, round(avg(averageRating), 2) AS avg_rating
            FROM titles
            WHERE averageRating IS NOT NULL
            GROUP BY titleType, decade
            ORDER BY titleType, decade
        """,
        "clickhouse_sql": """
            SELECT titleType, decade, count() AS num_titles, round(avg(averageRating), 2) AS avg_rating
            FROM imdb.titles
            WHERE averageRating IS NOT NULL
            GROUP BY titleType, decade
            ORDER BY titleType, decade
        """,
    },
    {
        "name": "top_rated_popular_series",
        "description": "Top 10 highest-rated tvSeries with >10k votes (filter + sort + limit)",
        "spark_sql": """
            SELECT tconst, primaryTitle, averageRating, numVotes
            FROM titles
            WHERE titleType = 'tvSeries' AND numVotes > 10000
            ORDER BY averageRating DESC
            LIMIT 10
        """,
        "clickhouse_sql": """
            SELECT tconst, primaryTitle, averageRating, numVotes
            FROM imdb.titles
            WHERE titleType = 'tvSeries' AND numVotes > 10000
            ORDER BY averageRating DESC
            LIMIT 10
        """,
    },
    {
        "name": "top_genres_since_2010",
        "description": "Most common genres since 2010 by title count (array explosion)",
        "spark_sql": """
            SELECT genre, count(*) AS num_titles, round(avg(averageRating), 2) AS avg_rating
            FROM (SELECT explode(genres) AS genre, averageRating FROM titles WHERE decade >= 2010)
            GROUP BY genre
            ORDER BY num_titles DESC
            LIMIT 10
        """,
        "clickhouse_sql": """
            SELECT genre, count() AS num_titles, round(avg(averageRating), 2) AS avg_rating
            FROM imdb.titles
            ARRAY JOIN genres AS genre
            WHERE decade >= 2010
            GROUP BY genre
            ORDER BY num_titles DESC
            LIMIT 10
        """,
    },
    {
        "name": "series_with_most_episodes",
        "description": "Parent series with the most episodes + avg episode rating",
        "spark_sql": """
            SELECT parentTconst, count(*) AS num_episodes, round(avg(averageRating), 2) AS avg_rating
            FROM titles
            WHERE titleType = 'tvEpisode' AND parentTconst IS NOT NULL
            GROUP BY parentTconst
            ORDER BY num_episodes DESC
            LIMIT 10
        """,
        "clickhouse_sql": """
            SELECT parentTconst, count() AS num_episodes, round(avg(averageRating), 2) AS avg_rating
            FROM imdb.titles
            WHERE titleType = 'tvEpisode' AND parentTconst IS NOT NULL
            GROUP BY parentTconst
            ORDER BY num_episodes DESC
            LIMIT 10
        """,
    },
]
