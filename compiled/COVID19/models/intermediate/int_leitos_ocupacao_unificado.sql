-- models/intermediate/int_leitos_ocupacao_unificado.sql

-- Este modelo serve como ponte, enriquecendo os dados de staging com
-- as chaves das dimensões antes de carregar a tabela de fatos.

WITH staging_data AS (
    SELECT * FROM COVID19.bronze.stg_leito_ocupacao_consolidado
),

dim_localidade AS (
    SELECT * FROM COVID19.gold.dim_localidade
)

SELECT
    stg.*,  -- Seleciona todas as colunas originais do staging
    loc.id_localidade -- Adiciona a chave da dimensão de localidade

FROM staging_data stg

-- Faz o JOIN com a dimensão de localidade usando os nomes de texto (como definido anteriormente)
-- para encontrar o ID correto.
LEFT JOIN dim_localidade loc ON
    UPPER(COALESCE(stg.municipio_notificacao, stg.municipio, 'Desconhecido')) = UPPER(loc.municipio)
    AND UPPER(COALESCE(stg.estado_notificacao, stg.estado, 'Desconhecido')) = UPPER(loc.estado)