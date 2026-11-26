# Home advantage and bookmaker calibration in European football

## Authors
- Martin Ratcliffe
- Pablo Gatsis

## Prerequisites
- Onyxia `vscode-pyspark` service (provides `aws` CLI, DuckDB CLI, Spark)
- S3 credentials loaded automatically from the Onyxia `AWS_*` environment variables (no secret in this repository)

## Dataset
| | |
|---|---|
| Name | Football results and betting odds (football-data.co.uk) |
| URL | https://www.football-data.co.uk/data.php |
| Licence | No formal licence. Site terms: free for private individuals, not for commercial use or data-training products using bots/scrapers/AI. Used here for a non-commercial student project; files downloaded once (checked 2026-11-26). |
| Download date | 2026-11-26 |
| Scope | Premier League (E0), Ligue 1 (F1), La Liga (SP1), seasons 2015-16 to 2024-25 |
| Size | 30 CSV files, 4.0 MiB, 11,151 matches |

## Research question
Has home advantage in the Premier League, Ligue 1 and La Liga changed since 2015, especially during the matches played without spectators?
Do bookmakers' odds price it correctly: does a team given a 60% implied probability actually win about 60% of the time?
We compare actual home win rates with odds-implied probabilities over 10 seasons.

## S3 paths
| Layer | Path |
|---|---|
| Bronze (raw, unmodified) | `s3://user-m-ratcliffe-ece/raw/football-data/` |
| Silver | TODO |
| Gold | TODO |

## Reproduction
```bash
bash src/download.sh                          # download the 30 raw CSV files into raw/
bash src/upload_s3.sh                         # upload them unchanged to S3 (bronze)
duckdb -c ".read src/quality_checks.sql"      # data quality checks on bronze
# TODO: silver (Spark job), gold (SQL), independent verification
```
