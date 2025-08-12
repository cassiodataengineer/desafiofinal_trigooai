WITH date_spine AS (
 SELECT DISTINCT DATE(data_notificacao) AS data
 FROM COVID19.bronze.stg_leito_ocupacao_consolidado
 WHERE data_notificacao IS NOT NULL
)
SELECT
 TO_NUMBER(TO_CHAR(data, 'YYYYMMDD')) AS id_tempo,
 data,
 EXTRACT(DAY FROM data) AS dia,
 EXTRACT(MONTH FROM data) AS mes,
 MONTHNAME(data) AS nome_mes,
 EXTRACT(YEAR FROM data) AS ano,
 EXTRACT(DAYOFWEEK FROM data) AS dia_da_semana,
 DAYNAME(data) AS nome_dia_da_semana,
 EXTRACT(QUARTER FROM data) AS trimestre,
 EXTRACT(WEEK FROM data) AS semana_do_ano,
 FALSE AS feriado -- Pode ser populado posteriormente com lógica de feriados
FROM date_spine
ORDER BY data