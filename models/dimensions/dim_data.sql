-- models/dimensions/dim_data.sql
WITH datas_distintas AS (
    -- Pega todas as datas únicas dos seus dados de leitos
    SELECT DISTINCT CAST(data_notificacao AS DATE) AS data
    FROM {{ ref('stg_leito_ocupacao_consolidado') }}
)
SELECT
    -- Cria um ID numérico para a data (ex: 20210131)
    TO_CHAR(data, 'YYYYMMDD')::INT AS id_tempo,
    data,
    EXTRACT(YEAR FROM data) AS ano,
    EXTRACT(MONTH FROM data) AS mes,
    EXTRACT(DAY FROM data) AS dia,
    EXTRACT(DAYOFWEEK FROM data) AS dia_da_semana
FROM datas_distintas
ORDER BY data
