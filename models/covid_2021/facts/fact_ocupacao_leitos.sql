WITH staging_data AS (
 SELECT * FROM {{ ref('stg_leito_ocupacao_2021') }}
 {% if is_incremental() %}
 WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
 {% endif %}
),
unpivoted_data AS (
 SELECT
 id_registro,
 data_notificacao,
 cnes,
 estado_notificacao,
 municipio_notificacao,
 estado,
 municipio,
 updated_at,
 'Suspeito' AS tipo_ocupacao,
 'Clínico' AS tipo_leito,
 ocupacao_suspeito_cli AS ocupacao_leitos,
 saida_suspeita_obitos AS saida_obitos,
 saida_suspeita_altas AS saida_altas
 FROM staging_data

 UNION ALL

 SELECT
 id_registro,
 data_notificacao,
 cnes,
 estado_notificacao,
 municipio_notificacao,
 estado,
 municipio,
 updated_at,
 'Suspeito' AS tipo_ocupacao,
 'UTI' AS tipo_leito,
 ocupacao_suspeito_uti AS ocupacao_leitos,
 saida_suspeita_obitos AS saida_obitos,
 saida_suspeita_altas AS saida_altas
 FROM staging_data

)
SELECT
 ROW_NUMBER() OVER (ORDER BY u.id_registro, u.tipo_ocupacao, u.tipo_leito)
AS id_fato,
 u.id_registro,
 t.id_tempo,
 l.id_localidade,
 u.cnes AS id_unidade_saude,
 ot.id_ocupacao_tipo,
 u.ocupacao_leitos,
 u.saida_obitos,
 u.saida_altas,
 u.updated_at
FROM unpivoted_data u
JOIN {{ ref('dim_tempo') }} t ON DATE(u.data_notificacao) = t.data
JOIN {{ ref('dim_localidade') }} l ON
 COALESCE(u.estado_notificacao, u.estado, 'Desconhecido') = l.estado AND
 COALESCE(u.municipio_notificacao, u.municipio, 'Desconhecido') =
l.municipio
JOIN {{ ref('dim_ocupacao_tipo') }} ot ON
 u.tipo_ocupacao = ot.tipo_ocupacao AND
 u.tipo_leito = ot.tipo_leito
WHERE u.ocupacao_leitos > 0 OR u.saida_obitos > 0 OR u.saida_altas > 0