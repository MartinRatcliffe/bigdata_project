-- Vue sur tous les CSV bruts de S3, sans les copier
CREATE OR REPLACE VIEW bronze AS
SELECT * FROM read_csv(
  's3://user-m-ratcliffe-ece/raw/football-data/*.csv',
  union_by_name = true,   -- colonnes différentes selon les saisons : on les aligne par nom
  all_varchar   = true,   -- tout en texte : on regarde le brut sans conversion
  filename      = true    -- ajoute une colonne avec le nom du fichier source
);

-- 1. Matchs et lignes vides par fichier
SELECT regexp_extract(filename, '(\d{4}_\w+)\.csv', 1) AS file,
       count(*) AS rows,
       count(*) FILTER (WHERE HomeTeam IS NULL) AS empty_rows
FROM bronze GROUP BY file ORDER BY file;

-- 2. Formats de date : 8 caractères = jj/mm/aa, 10 = jj/mm/aaaa
SELECT length(Date) AS date_length, count(*) AS rows
FROM bronze WHERE Date IS NOT NULL GROUP BY 1 ORDER BY 1;

-- 3. Cotes manquantes : Bet365 et les deux noms de la cote moyenne
SELECT regexp_extract(filename, '(\d{4})_', 1) AS season,
       count(*) FILTER (WHERE B365H IS NULL) AS missing_b365,
       count(*) FILTER (WHERE AvgH  IS NULL) AS missing_avg,
       count(*) FILTER (WHERE BbAvH IS NULL) AS missing_bbav
FROM bronze WHERE HomeTeam IS NOT NULL GROUP BY 1 ORDER BY 1;

-- 4. Doublons : un même match présent deux fois
SELECT Div, Date, HomeTeam, AwayTeam, count(*) AS n
FROM bronze WHERE HomeTeam IS NOT NULL
GROUP BY ALL HAVING count(*) > 1;