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
    decade          Int16               -- 0 = unknown (sentinel; PARTITION BY key can't be Nullable)
)
ENGINE = MergeTree
PARTITION BY decade                          -- coarse physical split: whole decades pruned/dropped wholesale
ORDER BY (titleType, startYear, tconst)       -- physical sort order + sparse primary index
PRIMARY KEY (titleType, startYear, tconst);   -- explicit for clarity (defaults to ORDER BY prefix otherwise)
