SELECT *
FROM {{ ref('fact_ocupacao_leitos') }} f
JOIN {{ ref('dim_tempo') }} t ON f.id_tempo = t.id_tempo
WHERE t.data > CURRENT_DATE()
