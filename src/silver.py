"""Silver : lit les CSV bruts (Bronze) sur S3, nettoie, fusionne, écrit en Parquet sur S3."""
import os
from functools import reduce
from pyspark.sql import SparkSession, functions as F

BUCKET = os.environ.get("BUCKET") or os.environ["KUBERNETES_NAMESPACE"]
BRONZE = f"s3a://{BUCKET}/raw/football-data"
SILVER = f"s3a://{BUCKET}/silver/matches"

SEASONS = ["1516", "1617", "1718", "1819", "1920", "2021", "2122", "2223", "2324", "2425"]
LEAGUES = {"E0": "Premier League", "F1": "Ligue 1", "SP1": "La Liga"}
COLS = ["Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR", "B365H", "B365D", "B365A"]

# Session Spark : configuration S3 reprise du lab 7 (lab-pyspark-silver)
spark = (
    SparkSession.builder
    .appName("silver-matches")
    .master("local[2]")
    .config("spark.driver.memory", "768m")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# 1. Lire fichier par fichier, garder les colonnes utiles, aligner par nom
frames = []
for season in SEASONS:
    for code, name in LEAGUES.items():
        df = (spark.read.option("header", True)
              .csv(f"{BRONZE}/{season}_{code}.csv")
              .select(*COLS)
              .withColumn("season", F.lit(f"20{season[:2]}-{season[2:]}"))
              .withColumn("league_code", F.lit(code))
              .withColumn("league", F.lit(name)))
        frames.append(df)
bronze = reduce(lambda a, b: a.unionByName(b), frames)
print("Bronze rows:", bronze.count())

# 2. Nettoyer et typer
silver = (bronze
    .filter(F.col("HomeTeam").isNotNull())                  # supprime la ligne vide
    .withColumn("match_date", F.coalesce(                   # 2 formats de date
        F.try_to_date("Date", "dd/MM/yyyy"),
        F.try_to_date("Date", "dd/MM/yy")))
    .select(
        "league_code", "league", "season", "match_date",
        F.col("HomeTeam").alias("home_team"),
        F.col("AwayTeam").alias("away_team"),
        F.col("FTHG").try_cast("int").alias("home_goals"),
        F.col("FTAG").try_cast("int").alias("away_goals"),
        F.col("FTR").alias("result"),                       # H, D ou A
        F.col("B365H").try_cast("double").alias("odds_home"),
        F.col("B365D").try_cast("double").alias("odds_draw"),
        F.col("B365A").try_cast("double").alias("odds_away"))
    .dropDuplicates(["league_code", "match_date", "home_team", "away_team"]))
silver = silver.cache()

# 3. Contrôles avant écriture
print("Dates non lues:", silver.filter(F.col("match_date").isNull()).count())
print("Cotes Bet365 manquantes:", silver.filter(F.col("odds_home").isNull()).count())

# 4. Écrire en Parquet, un dossier par ligue
silver.write.mode("overwrite").partitionBy("league_code").parquet(SILVER)

# 5. Relire ce qui a été écrit pour vérifier
check = spark.read.parquet(SILVER)
print("Silver rows:", check.count())
check.groupBy("league", "season").count().orderBy("league", "season").show(30)
spark.stop()