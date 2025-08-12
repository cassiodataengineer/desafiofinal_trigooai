-- CTE para ler os dados da tabela de consulta de estabelecimentos
WITH estabelecimentos_cnes AS (
    SELECT
        -- Atenção: verifique se os nomes das colunas aqui são os corretos da sua tabela
        CAST(CO_CNES AS STRING) AS id_cnes,
        NO_FANTASIA AS nm_estabelecimento
    FROM
        -- AQUI ESTÁ A CORREÇÃO: Apontando para a tabela correta de estabelecimentos
        COVID19.BRONZE.RAW_ESTABELECIMENTOS_CNES
),

-- CTE para pegar todos os códigos CNES únicos dos seus dados de leitos
cnes_nos_dados AS (
    SELECT DISTINCT
        cnes AS id_cnes
    FROM
         COVID19.bronze.stg_leito_ocupacao_consolidado
    WHERE
        cnes IS NOT NULL
)

-- Junção para criar a dimensão final
SELECT
    c.id_cnes,
    COALESCE(cnes.nm_estabelecimento, 'Não Informado') AS nm_estabelecimento
FROM
    cnes_nos_dados c
LEFT JOIN
    estabelecimentos_cnes cnes ON c.id_cnes = cnes.id_cnes