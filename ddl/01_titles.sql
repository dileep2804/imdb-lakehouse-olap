CREATE DATABASE IF NOT EXISTS imdb;

CREATE TABLE IF NOT EXISTS imdb.titles
(
    tconst          String,
    primaryTitle    String,
    originalTitle   String,
    isAdult         Bool,
    startYear       UInt16,             -- 0 = unknown (sentinel; ORDER BY key can't be Nullable)
    endYear         Nullable(UInt16),
    runtimeMinutes  Nullable(UInt16),
    genres          Array(String),
    averageRating   Nullable(Float32),
    numVotes        Nullable(UInt32),
    parentTconst    Nullable(String),
    seasonNumber    Nullable(UInt16),
    episodeNumber   Nullable(UInt16),
    titleType       LowCardinality(String),
    decade          Int16,              -- 0 = unknown (sentinel; PARTITION BY key can't be Nullable)

    -- secondary "skip" indexes: cheap per-granule (8192-row block) checks that
    -- let ClickHouse skip whole granules without scanning them, for columns
    -- that aren't part of the ORDER BY prefix below
    INDEX idx_rating averageRating TYPE minmax GRANULARITY 4,
    INDEX idx_votes numVotes TYPE minmax GRANULARITY 4,
    INDEX idx_genres genres TYPE bloom_filter GRANULARITY 4
)
ENGINE = MergeTree
PARTITION BY decade                          -- coarse physical split: whole decades pruned/dropped wholesale
ORDER BY (titleType, startYear, tconst)       -- physical sort order + sparse primary index
PRIMARY KEY (titleType, startYear, tconst);   -- explicit for clarity (defaults to ORDER BY prefix otherwise)
