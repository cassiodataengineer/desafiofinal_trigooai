SELECT *
FROM COVID19.gold.fact_ocupacao_leitos f
JOIN COVID19.gold.dim_tempo t ON f.id_tempo = t.id_tempo
WHERE t.data > CURRENT_DATE()