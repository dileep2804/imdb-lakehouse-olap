# PROMPTS.md

**LLM used:** Claude (Sonnet 5, model id `claude-sonnet-5`), via Claude Code CLI.

---

1. > "I'm choosing between ClickHouse and Redshift Spectrum for the OLAP engine. My stack is local/Docker-based. I know Spark well but not ClickHouse. Which one makes more sense and why? I need considerations around setup complexity, query latency, and suitability for this kind of analytical workload on ~1M rows."

2. > "I need to build an IMDb ETL pipeline with PySpark landing data in a lakehouse, then load to an OLAP engine for analytics, and benchmark raw Spark SQL vs the OLAP engine. The data comes from Kaggle (title.basics, title.ratings, name.basics). Give me a project structure with Docker Compose for Spark and ClickHouse."

3. > "Show me the ClickHouse DDL for loading IMDb title.basics data. I need a MergeTree table with an appropriate ordering key for analytical queries like 'top rated movies by decade' and 'most prolific actors'. Explain why you chose the partition key and ordering."

4. > "I'm planning to compare Spark SQL vs ClickHouse on 4 queries — top-rated movies by decade, most prolific actors, genre distribution, and runtime stats. What meaningful metrics should I capture beyond wall-clock time? Should I account for Spark cold starts? How many runs should I average?"

5. > "Generate a README with setup instructions and a PROMPTS.md containing all AI prompts used"

6. > "Some Debugging prompts like, kaggle doesn't contain required datasets, where else to download from?, debug why ClickHouse DDL loading is failing, etc.."
