-- Este modelo consolida os dados de ocupação de leitos de todos os anos.
SELECT * FROM COVID19.bronze.stg_leito_ocupacao_2020
UNION ALL
SELECT * FROM COVID19.bronze.stg_leito_ocupacao_2021
UNION ALL
SELECT * FROM COVID19.bronze.stg_leito_ocupacao_2022