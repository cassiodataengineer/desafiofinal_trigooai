SELECT * FROM (
 VALUES
 (1, 'Suspeito', 'Clínico'),
 (2, 'Suspeito', 'UTI'),
 (3, 'Confirmado', 'Clínico'),
 (4, 'Confirmado', 'UTI'),
 (5, 'COVID', 'Clínico'),
 (6, 'COVID', 'UTI'),
 (7, 'Hospitalar', 'Clínico'),
 (8, 'Hospitalar', 'UTI')
) AS t(id_ocupacao_tipo, tipo_ocupacao, tipo_leito)