#!/usr/bin/env bash
set -euo pipefail

# Requires a Kaggle API token at ~/.kaggle/kaggle.json
# (kaggle.com -> account settings -> "Create New API Token")
pip install -q kaggle

kaggle datasets download -d ashirwadsangwan/imdb-dataset -p data/raw --unzip
ls -la data/raw
