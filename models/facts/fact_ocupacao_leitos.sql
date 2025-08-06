-- models/facts/fact_ocupacao_leitos.sql

-- Passo 1: Referenciar a camada intermediária, que já deve ter o ID da localidade.
-- (Assumindo que você tem um modelo 'int_leitos_ocupacao_unificado' que já fez o join com dim_localidade para obter o id_localidade)
WITH intermediate_data AS (
    SELECT 
        *
    FROM {{ ref('int_leitos_ocupacao_unificado') }} -- ou ref('stg_leito_ocupacao_2021') se não tiver camada Silver
    
    {% if is_incremental() %}
    -- Lógica incremental para processar apenas dados novos
    WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
    {% endif %}
),

-- Passo 2: Fazer o UNPIVOT completo de TODAS as colunas de ocupação
unpivoted_data AS (

    -- Ocupação por COVID
    SELECT id_registro, data_notificacao, cnes, id_localidade, updated_at, 'COVID' AS tipo_ocupacao, 'Clínico' AS tipo_leito, ocupacao_covid_cli AS ocupacao FROM intermediate_data
    UNION ALL
    SELECT id_registro, data_notificacao, cnes, id_localidade, updated_at, 'COVID' AS tipo_ocupacao, 'UTI' AS tipo_leito, ocupacao_covid_uti AS ocupacao FROM intermediate_data
    
    UNION ALL

    -- Ocupação Hospitalar Total
    SELECT id_registro, data_notificacao, cnes, id_localidade, updated_at, 'Hospitalar' AS tipo_ocupacao, 'Clínico' AS tipo_leito, ocupacao_hospitalar_cli AS ocupacao FROM intermediate_data
    UNION ALL
    SELECT id_registro, data_notificacao, cnes, id_localidade, updated_at, 'Hospitalar' AS tipo_ocupacao, 'UTI' AS tipo_leito, ocupacao_hospitalar_uti AS ocupacao FROM intermediate_data

    -- Adicione aqui os outros tipos (Confirmado, Suspeito) se forem necessários para a análise
    
),

-- Passo 3: Trazer as métricas de saídas (óbito/alta) de volta para evitar duplicação
-- As saídas ocorrem uma vez por registro, não uma vez por tipo de leito.
saidas_data AS (
    SELECT
        id_registro,
        saida_confirmada_obitos,
        saida_confirmada_altas
    FROM intermediate_data
)

-- Passo 4: Montar a tabela de fatos final
SELECT
    -- Cria uma chave primária única para a tabela de fatos
    {{ dbt_utils.generate_surrogate_key(['u.id_registro', 'ot.id_ocupacao_tipo']) }} AS id_fato,
    
    -- Chaves de dimensão (Surrogate Keys)
    t.id_tempo,
    u.id_localidade,
    u.cnes AS id_cnes, -- Idealmente, aqui também teríamos uma dimensão e um ID numérico
    ot.id_ocupacao_tipo,
    
    -- Métricas
    u.ocupacao AS quantidade_leitos_ocupados,
    s.saida_confirmada_obitos,
    s.saida_confirmada_altas,

    -- Metadados
    u.updated_at

FROM unpivoted_data u

-- Join para buscar o id_tempo da dimensão de data
JOIN {{ ref('dim_data') }} t ON DATE(u.data_notificacao) = t.data

-- Join para buscar o id_ocupacao_tipo da dimensão de tipos
JOIN {{ ref('dim_ocupacao_tipo') }} ot ON u.tipo_ocupacao = ot.tipo_ocupacao AND u.tipo_leito = ot.tipo_leito

-- Join para trazer as métricas de saída sem duplicá-las
LEFT JOIN saidas_data s ON u.id_registro = s.id_registro

-- Filtra registros que não têm métricas relevantes
WHERE u.ocupacao > 0