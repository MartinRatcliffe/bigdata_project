#!/usr/bin/env bash
# Télécharge les CSV football-data.co.uk dans raw/ (non modifiés)
set -euo pipefail
mkdir -p raw
for season in 1516 1617 1718 1819 1920 2021 2122 2223 2324 2425aws s3 ls --endpoint-url https://$AWS_S3_ENDPOINT; do
  for league in E0 F1 SP1; do
    url="https://www.football-data.co.uk/mmz4281/${season}/${league}.csv"
    curl -fsSL -o "raw/${season}_${league}.csv" "$url"
    echo "OK ${season}_${league}"
  done
done
