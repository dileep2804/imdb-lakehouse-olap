#!/usr/bin/env bash
set -euo pipefail

# Requires a Kaggle API token at ~/.kaggle/kaggle.json
pip install -q kaggle

mkdir -p data/raw
kaggle datasets download -d ashirwadsangwan/imdb-dataset -f title.basics.tsv -p data/raw --force
kaggle datasets download -d ashirwadsangwan/imdb-dataset -f title.ratings.tsv -p data/raw --force

# title.episode isn't in the Kaggle mirror - pulled directly from IMDb's own
curl -o data/raw/title.episode.tsv.gz https://datasets.imdbws.com/title.episode.tsv.gz

ls -la data/raw
