INSERT INTO happiness (country, rank, score)
    SELECT DISTINCT country,
           rank,
           score
    FROM staging_happiness;