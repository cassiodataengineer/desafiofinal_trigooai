Projeto COVID-19: Análise de Ocupação de Leitos com dbt e Snowflake 

1. Introdução 

Este documento detalha o projeto de análise de ocupação de leitos hospitalares relacionados à COVID-19, utilizando dbt (data build tool) para transformações de dados e Snowflake como data warehouse. O objetivo principal é consolidar e preparar dados brutos de ocupação de leitos de diversos anos (2020, 2021 e 2022) em um formato estruturado e otimizado para análises, relatórios e dashboards. A metodologia empregada garante a qualidade, consistência e acessibilidade dos dados, permitindo uma compreensão aprofundada da dinâmica de ocupação de leitos ao longo do tempo. A documentação completa do dbt para este projeto pode ser acessada em: (https://kk400.us1.dbt.com/accounts/70471823483193/develop/70471824053089/docs/index.html#!/overview/dbt_utils)
2. Visão Geral do Projeto 

O projeto aborda o desafio de integrar e harmonizar dados heterogêneos de ocupação de leitos hospitalares. A complexidade reside na variedade de fontes e na necessidade de padronização para análises comparativas. Através do dbt, implementamos um pipeline de transformação que ingere dados brutos, aplica regras de limpeza e padronização, e os organiza em um modelo de dados dimensional, facilitando o consumo por ferramentas de Business Intelligence (BI) e análises exploratórias. Este processo é fundamental para converter um volume massivo de informações em insights acionáveis para a gestão de saúde pública. 

3. Arquitetura de Dados: Camadas de Refinamento 

A arquitetura de dados do projeto é baseada em um design de três camadas, que promove a modularidade, reusabilidade e governança dos dados. Cada camada representa um estágio progressivo de refinamento e agregação, garantindo que os dados sejam transformados de sua forma bruta para um formato pronto para consumo, mantendo a rastreabilidade e a qualidade. Esta abordagem em camadas é uma prática recomendada em engenharia de dados, pois isola as complexidades e permite que diferentes equipes trabalhem em diferentes estágios do pipeline sem interferência. 

3.1. Camada BRONZE 

A camada BRONZE serve como a área de aterrissagem inicial para os dados brutos. Nesta fase, os dados são carregados do ambiente de origem para o Snowflake com o mínimo de transformações possível. O foco principal é a ingestão e a padronização básica, como a correção de tipos de dados e a remoção de caracteres inválidos. É crucial que esta camada mantenha uma representação fiel dos dados originais, atuando como um histórico imutável das informações de entrada. A separação dos dados por ano (2020, 2021, 2022) nesta camada facilita a gestão e a auditoria de cada conjunto de dados de forma independente antes da consolidação.

3.2. Camada SILVER 

A camada SILVER é onde as transformações intermediárias e o enriquecimento dos dados ocorrem. Os dados da camada BRONZE são combinados e integrados, resolvendo inconsistências e aplicando regras de negócio mais complexas. Por exemplo, a unificação de dados de diferentes anos e a junção com tabelas de referência (como municípios e estabelecimentos de saúde) são realizadas nesta camada. O objetivo é criar um conjunto de dados mais limpo, consistente e consolidado, que serve como base para a camada final de consumo. Esta camada é essencial para garantir a integridade e a coerência dos dados antes que sejam expostos aos usuários finais. 

3.3. Camada GOLD 

A camada GOLD é a camada final e de consumo, onde os dados são apresentados em um formato otimizado para análise e relatórios. Nesta camada, os dados são modelados em tabelas de fatos e dimensões, seguindo os princípios da modelagem dimensional. As tabelas de fatos contêm as métricas quantitativas (e.g., número de leitos ocupados, óbitos), enquanto as tabelas de dimensão fornecem o contexto descritivo (e.g., data, localização, tipo de ocupação). Esta estrutura facilita a navegação e a consulta dos dados por analistas de negócio e ferramentas de BI, permitindo a criação de dashboards e relatórios de forma eficiente e intuitiva. A camada GOLD é a interface direta para a tomada de decisões baseada em dados. 

4. Ingestão e Automação de Dados (Snowflake) 

A ingestão de dados para o Snowflake é um passo crítico no pipeline, garantindo que os dados brutos estejam disponíveis para as transformações do dbt. Utilizamos scripts SQL para criar as tabelas RAW na camada BRONZE e carregar os dados a partir de arquivos CSV armazenados em estágios (stages) do Snowflake. A automação deste processo é fundamental para manter os dados atualizados e garantir a eficiência operacional. Além dos comandos 

COPY INTO para carregamento inicial, a utilização de Snowflake Tasks e Streams pode ser implementada para automatizar a ingestão incremental de novos dados, minimizando a latência e o esforço manual. 

4.1. Criação de Tabelas RAW 

As tabelas RAW são as primeiras a serem criadas no esquema BRONZE do Snowflake. Elas espelham a estrutura dos arquivos CSV de origem, garantindo que todos os campos sejam capturados sem perda de informação. A definição de tipos de dados apropriados é crucial nesta etapa para evitar erros durante o carregamento. Exemplos de criação de tabelas para dados de ocupação de leitos e tabelas de referência são fornecidos abaixo:

-- Tabela para os dados brutos de 2020 

CREATE OR REPLACE TABLE 
COVID19.BRONZE.RAW_LEITO_OCUPACAO_2020 ( UNNAMED_O NUMBER(38,0), 
_ID VARCHAR(16777216), 
DATA_NOTIFICACAO TIMESTAMP_NTZ(9), 
CNES VARCHAR(16777216), 
OCUPACAO_SUSPEITO_CLI FLOAT, 
OCUPACAO_SUSPEITO_UTI FLOAT, 
OCUPACAO_CONFIRMADO_CLI FLOAT, 
OCUPACAO_CONFIRMADO_UTI FLOAT, 
OCUPACAO_COVID_UTI FLOAT, 
OCUPACAO_COVID_CLI FLOAT, 
OCUPACAO_HOSPITALAR_UTI FLOAT, 
OCUPACAO_HOSPITALAR_CLI FLOAT, 
SAIDA_SUSPEITA_OBITOS FLOAT, 
SAIDA_SUSPEITA_ALTAS FLOAT, 
SAIDA_CONFIRMADA_OBITOS FLOAT, 
SAIDA_CONFIRMADA_ALTAS FLOAT, 
ORIGEM VARCHAR(16777216), 
P_USUARIO VARCHAR(16777216), 
ESTADO_NOTIFICACAO VARCHAR(16777216), 
MUNICIPIO_NOTIFICACAO VARCHAR(16777216), 
ESTADO VARCHAR(16777216), 
MUNICIPIO VARCHAR(16777216), 
EXCLUIDO BOOLEAN, 
VALIDADO BOOLEAN, 
CREATED_AT TIMESTAMP_NTZ(9), 

); 
UPDATED_AT TIMESTAMP_NTZ(9) 

-- Tabela para os dados brutos de 2021 

CREATE OR REPLACE TABLE COVID19.BRONZE.RAW_LEITO_OCUPACAO_2021 ( UNNAMED_O NUMBER(38,0), 
_ID VARCHAR(16777216), 
DATA_NOTIFICACAO TIMESTAMP_NTZ(9), 
CNES VARCHAR(16777216), 
OCUPACAO_SUSPEITO_CLI FLOAT, 
OCUPACAO_SUSPEITO_UTI FLOAT, 
OCUPACAO_CONFIRMADO_CLI FLOAT, 
OCUPACAO_CONFIRMADO_UTI FLOAT, 
OCUPACAO_COVID_UTI FLOAT, 
OCUPACAO_COVID_CLI FLOAT, 
OCUPACAO_HOSPITALAR_UTI FLOAT, 
OCUPACAO_HOSPITALAR_CLI FLOAT, 
SAIDA_SUSPEITA_OBITOS FLOAT, 
SAIDA_SUSPEITA_ALTAS FLOAT, 
SAIDA_CONFIRMADA_OBITOS FLOAT, 
SAIDA_CONFIRMADA_ALTAS FLOAT, 
ORIGEM VARCHAR(16777216), 
P_USUARIO VARCHAR(16777216), 
ESTADO_NOTIFICACAO VARCHAR(16777216), 
MUNICIPIO_NOTIFICACAO VARCHAR(16777216), 
ESTADO VARCHAR(16777216), 
MUNICIPIO VARCHAR(16777216), 
EXCLUIDO BOOLEAN, 
VALIDADO BOOLEAN, 
CREATED_AT TIMESTAMP_NTZ(9), 
UPDATED_AT TIMESTAMP_NTZ(9) 
); 

-- Tabela para os dados brutos de 2022 

CREATE OR REPLACE TABLE COVID19.BRONZE.RAW_LEITO_OCUPACAO_2022 ( UNNAMED_O NUMBER(38,0), 
_ID VARCHAR(16777216), 
DATA_NOTIFICACAO TIMESTAMP_NTZ(9), 
CNES VARCHAR(16777216), 
OCUPACAO_SUSPEITO_CLI FLOAT, 
OCUPACAO_SUSPEITO_UTI FLOAT, 
OCUPACAO_CONFIRMADO_CLI FLOAT, 
OCUPACAO_CONFIRMADO_UTI FLOAT, 
OCUPACAO_COVID_UTI FLOAT, 
OCUPACAO_COVID_CLI FLOAT, 
OCUPACAO_HOSPITALAR_UTI FLOAT, 
OCUPACAO_HOSPITALAR_CLI FLOAT, 
SAIDA_SUSPEITA_OBITOS FLOAT, 
SAIDA_SUSPEITA_ALTAS FLOAT, 
SAIDA_CONFIRMADA_OBITOS FLOAT, 
SAIDA_CONFIRMADA_ALTAS FLOAT, 
ORIGEM VARCHAR(16777216), 
P_USUARIO VARCHAR(16777216), 
ESTADO_NOTIFICACAO VARCHAR(16777216),
); 
MUNICIPIO_NOTIFICACAO VARCHAR(16777216), ESTADO VARCHAR(16777216), 
MUNICIPIO VARCHAR(16777216), 
EXCLUIDO BOOLEAN, 
VALIDADO BOOLEAN, 
CREATED_AT TIMESTAMP_NTZ(9), 
UPDATED_AT TIMESTAMP_NTZ(9) 

-- Tabela para os dados brutos de Municípios IBGE CREATE OR REPLACE TABLE COVID19.BRONZE.RAW_MUNICIPIOS_IBGE ( CODIGO_MUNICIPIO VARCHAR(16777216), 

NOME_MUNICIPIO VARCHAR(16777216), 
UF VARCHAR(16777216), 
); 
CODIGO_UF VARCHAR(16777216) 

-- Tabela para os dados brutos de Estabelecimentos 

CNES CREATE OR REPLACE TABLE COVID19.BRONZE.RAW_ESTABELECIMENTOS_CNES ( CO_CNES VARCHAR(16777216), 
NO_FANTASIA VARCHAR(16777216), 
TP_GESTAO VARCHAR(16777216), 
); 
CO_CEP VARCHAR(16777216) 

4.2. Carregamento de Dados (COPY INTO) 

Após a criação das tabelas RAW, os dados são carregados a partir dos arquivos CSV localizados em estágios do Snowflake. O comando COPY INTO é otimizado para carregamento em massa e oferece opções para tratamento de erros e formatação de arquivos. É importante configurar corretamente o FILE_FORMAT para garantir que os dados sejam interpretados corretamente. A opção ON_ERROR = 'CONTINUE' é particularmente útil para ambientes de desenvolvimento, permitindo que o carregamento continue mesmo que algumas linhas apresentem erros, que podem ser investigados posteriormente.

-- Carregar dados para a tabela RAW_LEITO_OCUPACAO_2020 COPY INTO COVID19.BRONZE.RAW_LEITO_OCUPACAO_2020 
FROM @COVID19.BRONZE.LEITO_OCUPACAO/esus-vepi.LeitoOcupacao_2020.csv FILE_FORMAT = ( 
TYPE = CSV, 
FIELD_DELIMITER = 
SKIP_HEADER = 1, 
EMPTY_FIELD_AS_NULL = TRUE 

); 
ON_ERROR = 'CONTINUE' 

-- Carregar dados para a tabela 

RAW_LEITO_OCUPACAO_2021 COPY INTO COVID19.BRONZE.RAW_LEITO_OCUPACAO_2021 
FROM @COVID19.BRONZE.LEITO_OCUPACAO/esus-vepi.LeitoOcupacao_2021.csv FILE_FORMAT = ( 
TYPE = CSV, 
FIELD_DELIMITER = 
SKIP_HEADER = 1, 
EMPTY_FIELD_AS_NULL = TRUE 
); 
ON_ERROR = 'CONTINUE' 

-- Carregar dados para a tabela 

RAW_LEITO_OCUPACAO_2022 
COPY INTO COVID19.BRONZE.RAW_LEITO_OCUPACAO_2022 
FROM @COVID19.BRONZE.LEITO_OCUPACAO/esus-vepi.LeitoOcupacao_2022.csv 
FILE_FORMAT = ( 
TYPE = CSV, 
FIELD_DELIMITER = 
SKIP_HEADER = 1, 
EMPTY_FIELD_AS_NULL = TRUE 
ON_ERROR = 'CONTINUE' 
); 

-- Carregar dados para a tabela 

RAW_MUNICIPIOS_IBGE 
COPY INTO COVID19.BRONZE.RAW_MUNICIPIOS_IBGE 
FROM @COVID19.BRONZE.LEITO_OCUPACAO/municipios.csv 
FILE_FORMAT = ( 
TYPE = CSV, 
FIELD_DELIMITER = 
SKIP_HEADER = 1, 
EMPTY_FIELD_AS_NULL = TRUE 
ON_ERROR = 'CONTINUE' 
); 

-- Carregar dados para a tabela 

RAW_ESTABELECIMENTOS_CNES 
COPY INTO COVID19.BRONZE.RAW_ESTABELECIMENTOS_CNES 
FROM @COVID19.BRONZE.LEITO_OCUPACAO/cnes_estabelecimentos.csv 
FILE_FORMAT = ( 
TYPE = CSV, 
FIELD_DELIMITER = 
SKIP_HEADER = 1, 
EMPTY_FIELD_AS_NULL = TRUE 
ON_ERROR = 'CONTINUE' 
); 

4.3. Concessão de Privilégios 
Para que o dbt possa acessar as tabelas RAW e realizar as transformações, é necessário conceder os privilégios de leitura adequados às roles utilizadas pelo dbt no Snowflake. A segurança dos dados é primordial, e a concessão de privilégios mínimos necessários (princípio do menor privilégio) é uma prática recomendada. Neste caso, o privilégio SELECT é suficiente para que o dbt possa ler os dados das tabelas BRONZE .

-- Conceder privilégios de seleção nas tabelas RAW de ocupação de leitos 

GRANT SELECT ON TABLE COVID19.BRONZE.RAW_LEITO_OCUPACAO_2020 TO ROLE PC_DBT_DB_PICKER_ROLE; GRANT SELECT ON TABLE COVID19.BRONZE.RAW_LEITO_OCUPACAO_2020 TO ROLE PC_DBT_ROLE; 
GRANT SELECT ON TABLE COVID19.BRONZE.RAW_LEITO_OCUPACAO_2021 TO ROLE PC_DBT_DB_PICKER_ROLE; GRANT SELECT ON TABLE COVID19.BRONZE.RAW_LEITO_OCUPACAO_2021 TO ROLE PC_DBT_ROLE; 
GRANT SELECT ON TABLE COVID19.BRONZE.RAW_LEITO_OCUPACAO_2022 TO ROLE PC_DBT_DB_PICKER_ROLE; GRANT SELECT ON TABLE COVID19.BRONZE.RAW_LEITO_OCUPACAO_2022 TO ROLE PC_DBT_ROLE; 

-- Conceder privilégios de seleção nas tabelas RAW de enriquecimento 

GRANT SELECT ON TABLE COVID19.BRONZE.RAW_MUNICIPIOS_IBGE TO ROLE PC_DBT_DB_PICKER_ROLE; GRANT SELECT ON TABLE COVID19.BRONZE.RAW_MUNICIPIOS_IBGE TO ROLE PC_DBT_ROLE; 
GRANT SELECT ON TABLE COVID19.BRONZE.RAW_ESTABELECIMENTOS_CNES TO ROLE PC_DBT_DB_PICKER_ROLE; GRANT SELECT ON TABLE COVID19.BRONZE.RAW_ESTABELECIMENTOS_CNES TO ROLE PC_DBT_ROLE; 

5. Estrutura do Projeto dbt 

A organização do projeto dbt segue as convenções padrão, o que facilita a navegação e a manutenção do código. A estrutura de pastas é lógica e reflete as diferentes etapas do pipeline de transformação de dados. Cada diretório tem um propósito específico, garantindo que os modelos, macros e testes sejam facilmente localizados e gerenciados. A clareza na estrutura do projeto é vital para a colaboração em equipe e para a escalabilidade do projeto. 
. 
├── dbt_project.yml # Configurações gerais do projeto dbt. 
├── macros/ # Macros SQL reutilizáveis. 
│ └── generate_schema_name.sql # Macro para definição dinâmica de esquemas. 

├── models/ # Modelos SQL para transformação de dados. 
│ ├── staging/ # Modelos da camada BRONZE (staging). 
│ │ ├── stg_leito_ocupacao_2020.sql # Staging para dados de 2020. 
│ │ ├── stg_leito_ocupacao_2021.sql # Staging para dados de 2021. 
│ │ ├── stg_leito_ocupacao_2022.sql # Staging para dados de 2022. 
│ │ ├── stg_leito_ocupacao_consolidado.sql # Consolidação de dados de todos os anos. │ │ └── sources.yml # Definição das fontes de dados brutas. 
│ ├── intermediate/ # Modelos da camada SILVER (intermediária). 
│ │ └── int_leitos_ocupacao_unificado.sql # Unificação e enriquecimento de leitos. │ ├── dimensions/ # Modelos de dimensão da camada GOLD. 
│ │ ├── dim_cnes.sql 
│ │ ├── dim_data.sql 
│ │ ├── dim_localidade.sql 
│ │ ├── dim_ocupacao_tipo.sql 
│ │ ├── dim_tempo.sql 
│ │ └── dim_unidade_saude.sql 
│ └── facts/ # Modelos de fatos da camada GOLD. 
│ └── fact_ocupacao_leitos.sql 
└── tests/ # Testes de qualidade de dados. 
├── test_no_future_dates.sql # Exemplo de teste de data. 
└── schema.yml # Documentação e testes para os modelos. 

6. Componentes Essenciais do Projeto 

Cada arquivo e diretório no projeto dbt desempenha um papel crucial na orquestração e execução das transformações de dados. Compreender a função de cada componente é fundamental para a manutenção e o desenvolvimento contínuo do projeto. A seguir, detalhamos os principais arquivos e sua importância no fluxo de trabalho do dbt. 

6.1. dbt_project.yml : O Coração do Projeto 

O arquivo dbt_project.yml é o arquivo de configuração central do projeto dbt. Ele define metadados do projeto, como nome e versão, e especifica caminhos para os diferentes tipos de arquivos (modelos, macros, testes). Mais importante, ele configura como os modelos serão materializados (ou seja, como as tabelas e views serão criadas no data warehouse) para cada camada de dados. A materialização pode ser definida como view , table ,
incremental , ou ephemeral , cada uma com suas vantagens e desvantagens em termos de performance e custo. A configuração explícita de esquemas para cada camada ( bronze , silver , gold ) garante a organização e a separação lógica dos dados. 
name: 'COVID19' 
version: '1.0.0' 
config-version: 2 
profile: 'default' 
model-paths: ["models"] 
analysis-paths: ["analyses"] 
test-paths: ["tests"] 
seed-paths: ["seeds"] 
macro-paths: ["macros"] 
snapshot-paths: ["snapshots"] 
target-path: "target" 
clean-targets: 
- "target" 
- "dbt_packages" 
models: 
COVID19: 
staging: 
+materialized: view 
+schema: bronze 
intermediate: 
+materialized: table 
+schema: silver 
dimensions: 
+materialized: table 
+schema: gold 
facts: 
+materialized: incremental 
+schema: gold 

6.2. macros/generate_schema_name.sql : Gerenciamento Dinâmico de Esquemas 

Esta macro personalizada é responsável por definir dinamicamente o nome do esquema onde cada modelo será materializado. Ela utiliza a variável custom_schema_name definida no dbt_project.yml para direcionar os modelos para os esquemas bronze , silver ou gold . A flexibilidade desta macro permite que o projeto mantenha uma estrutura de esquemas consistente e padronizada, independentemente do ambiente (desenvolvimento, staging, produção). Isso é particularmente útil em ambientes multi-usuário ou multi-ambiente, onde diferentes esquemas podem ser necessários para isolar dados ou testar novas funcionalidades. 
{% macro generate_schema_name(custom_schema_name, node) -%} 
{# 
Esta macro define como os nomes dos esquemas serão gerados. 
- custom_schema_name: O nome do esquema que definimos no dbt_project.yml 

(ex: 'bronze', 'silver', 'gold'). 

- node: O objeto que representa o modelo atual que está sendo construído. 
#} 
{% set default_schema = target.schema -%} 
{%- if custom_schema_name is none -%} 
{{ default_schema }} 
{%- else -%} 
{{ custom_schema_name | trim }} 
{%-endif-%} 
{%- endmacro %} 

6.3. models/staging/sources.yml : Definição das Fontes de Dados 

O arquivo sources.yml é crucial para declarar as fontes de dados brutas que o dbt utilizará. Ele mapeia as tabelas existentes no Snowflake (neste caso, as tabelas RAW na camada BRONZE ) para que o dbt possa referenciá-las de forma segura e organizada. A declaração de fontes permite que o dbt construa um grafo de dependências completo, desde os dados brutos até os modelos finais, e também facilita a documentação automática e a
aplicação de testes de integridade de dados nas fontes. A inclusão dos dados de 2020, 2021 e 2022 neste arquivo garante que todas as fontes necessárias estejam disponíveis para as transformações subsequentes. 

version: 2 
sources: 
- name: bronze_source 
database: COVID19 
schema: BRONZE 
tables: 
- name: RAW_LEITO_OCUPACAO_2021 
- name: RAW_MUNICIPIOS_IBGE 
- name: RAW_ESTABELECIMENTOS_CNES 
- name: RAW_LEITO_OCUPACAO_2020 
- name: RAW_LEITO_OCUPACAO_2022 

6.4. Modelos de Staging ( stg_leito_ocupacao_XXXX.sql ) 

Os modelos na pasta staging são responsáveis pela primeira etapa de transformação dos dados brutos. Cada arquivo stg_leito_ocupacao_XXXX.sql (para 2020, 2021 e 2022) seleciona as colunas relevantes, renomeia-as para um padrão consistente e aplica limpezas básicas, como a conversão de tipos de dados e o tratamento de valores nulos ( COALESCE ). A adição da coluna ano_dados é fundamental para identificar a origem temporal de cada registro, permitindo a consolidação posterior. A cláusula WHERE excluido = FALSE garante que apenas os registros válidos sejam considerados nas etapas seguintes, melhorando a qualidade dos dados desde o início. 

-- models/staging/stg_leito_ocupacao_2020.sql 

SELECT 
_id AS id_registro, 
TO_TIMESTAMP_NTZ(data_notificacao) AS data_notificacao, 
TRIM(cnes) AS cnes, 
COALESCE(ocupacao_suspeito_cli, 0) AS ocupacao_suspeito_cli, 
COALESCE(ocupacao_suspeito_uti, 0) AS ocupacao_suspeito_uti, 
COALESCE(ocupacao_confirmado_cli, 0) AS ocupacao_confirmado_cli, 
COALESCE(ocupacao_confirmado_uti, 0) AS ocupacao_confirmado_uti, 
COALESCE(ocupacao_covid_uti, 0) AS ocupacao_covid_uti, 
COALESCE(ocupacao_covid_cli, 0) AS ocupacao_covid_cli, 
COALESCE(ocupacao_hospitalar_uti, 0) AS ocupacao_hospitalar_uti, 
COALESCE(ocupacao_hospitalar_cli, 0) AS ocupacao_hospitalar_cli, 
COALESCE(saida_suspeita_obitos, 0) AS saida_suspeita_obitos, 
COALESCE(saida_suspeita_altas, 0) AS saida_suspeita_altas, 
COALESCE(saida_confirmada_obitos, 0) AS saida_confirmada_obitos, 
COALESCE(saida_confirmada_altas, 0) AS saida_confirmada_altas, 
TRIM(origem) AS origem, 
TRIM(p_usuario) AS p_usuario, 
TRIM(estado_notificacao) AS estado_notificacao, 
TRIM(municipio_notificacao) AS municipio_notificacao, 
TRIM(estado) AS estado, 
TRIM(municipio) AS municipio, 
excluido, 
validado, 
created_at, 
updated_at, 
2020 AS ano_dados 
FROM {{ source('bronze_source', 'RAW_LEITO_OCUPACAO_2020') }} 
WHERE excluido = FALSE 

-- Modelos similares para 2021 e 2022 

(stg_leito_ocupacao_2021.sql e stg_leito_ocupacao_2022.sql) -- A estrutura é idêntica, alterando apenas a fonte de dados e o valor de 'ano_dados'. 

6.5. stg_leito_ocupacao_consolidado.sql : Unificação dos Dados de Staging 
Este modelo é responsável por unificar os dados de ocupação de leitos de todos os anos (2020, 2021, 2022) em uma única view. Utilizando a cláusula UNION ALL , ele combina os resultados dos modelos de staging individuais. Esta consolidação é um passo crucial para permitir análises que abrangem múltiplos anos, sem a necessidade de consultar tabelas separadas. A criação de uma view consolidada na camada BRONZE (via materialização view no
dbt_project.yml ) garante que os dados estejam sempre atualizados com as últimas informações de cada ano, sem duplicar o armazenamento físico. 

-- models/staging/stg_leito_ocupacao_consolidado.sql 

SELECT * 
FROM {{ ref('stg_leito_ocupacao_2020') }} 
UNION ALL 
SELECT * 
FROM {{ ref('stg_leito_ocupacao_2021') }} 
UNION ALL 
SELECT * 
FROM {{ ref('stg_leito_ocupacao_2022') }} 

6.6. int_leitos_ocupacao_unificado.sql : Enriquecimento e Unificação Intermediária 
O modelo int_leitos_ocupacao_unificado.sql na camada intermediate (SILVER) é onde o enriquecimento e a unificação mais complexa dos dados ocorrem. Ele une os dados consolidados de ocupação de leitos com as tabelas de referência de municípios e estabelecimentos CNES. Isso adiciona informações contextuais importantes, como nome do município, UF, nome fantasia do hospital e tipo de gestão. A utilização de LEFT JOIN garante que todos os registros de ocupação de leitos sejam mantidos, mesmo que não haja correspondência nas tabelas de referência. Este modelo é materializado como uma table , o que significa que os resultados são persistidos no Snowflake, otimizando o desempenho para consultas subsequentes. 

-- models/intermediate/int_leitos_ocupacao_unificado.sql 
SELECT 
stg.id_registro, 
stg.data_notificacao, 
stg.cnes, 
stg.ocupacao_suspeito_cli, 
stg.ocupacao_suspeito_uti, 
stg.ocupacao_confirmado_cli, 
stg.ocupacao_confirmado_uti, 
stg.ocupacao_covid_uti, 
stg.ocupacao_covid_cli, 
stg.ocupacao_hospitalar_uti, 
stg.ocupacao_hospitalar_cli, 
stg.saida_suspeita_obitos, 
stg.saida_suspeita_altas, 
stg.saida_confirmada_obitos, 
stg.saida_confirmada_altas, 
stg.origem, 
stg.p_usuario, 
stg.estado_notificacao, 
stg.municipio_notificacao, 
stg.estado, 
stg.municipio, 
stg.excluido, 
stg.validado, 
stg.created_at, 
stg.updated_at, 
stg.ano_dados, 
mun.NOME_MUNICIPIO AS nome_municipio_ibge, 
mun.UF AS uf_ibge, 
estab.NO_FANTASIA AS nome_fantasia_cnes, 
estab.TP_GESTAO AS tipo_gestao_cnes 
FROM {{ ref('stg_leito_ocupacao_consolidado') }} stg 
LEFT JOIN {{ source('bronze_source', 'RAW_MUNICIPIOS_IBGE') }} mun 
ON stg.municipio = mun.NOME_MUNICIPIO -- Assumindo que 'municipio' em stg corresponde a 'NOME_MUNICIPIO' em RAW_MUNICIPIOS_IBGE 
LEFT JOIN {{ source('bronze_source', 'RAW_ESTABELECIMENTOS_CNES') }} estab 
ON stg.cnes = estab.CO_CNES 

6.7. Modelos de Dimensão ( dimensions/ ) 

Os modelos de dimensão na camada GOLD fornecem o contexto para as métricas de ocupação de leitos. Cada dimensão é uma tabela que contém atributos descritivos e não voláteis. Por exemplo, dim_cnes contém informações sobre os estabelecimentos de saúde, dim_data sobre as datas, e dim_localidade sobre as
localizações geográficas. A criação de dimensões separadas promove a reusabilidade e a consistência dos dados, além de otimizar o desempenho de consultas ao permitir que as ferramentas de BI filtrem e agrupem dados de forma eficiente. Todos os modelos de dimensão são materializados como table . 

-- Exemplo: models/dimensions/dim_data.sql 

SELECT 
DISTINCT data_notificacao AS data_completa, 
YEAR(data_notificacao) AS ano, 
MONTH(data_notificacao) AS mes, 
DAY(data_notificacao) AS dia, 
DAYOFWEEK(data_notificacao) AS dia_da_semana, 
WEEKOFYEAR(data_notificacao) AS semana_do_ano, 
QUARTER(data_notificacao) AS trimestre, 
TO_CHAR(data_notificacao, 'YYYY-MM') AS ano_mes 
FROM {{ ref('int_leitos_ocupacao_unificado') }} 
ORDER BY data_completa 

-- Modelos similares para outras dimensões (dim_cnes, dim_localidade, etc.) 

6.8. Modelo de Fato ( fact_ocupacao_leitos.sql ) 
O modelo fact_ocupacao_leitos.sql é o coração da camada GOLD , contendo as métricas de ocupação de leitos e as chaves estrangeiras para as tabelas de dimensão. Este modelo é materializado como incremental , o que significa que apenas os novos dados são processados e adicionados à tabela existente em cada execução do dbt. Isso é crucial para conjuntos de dados grandes e em constante crescimento, pois reduz significativamente o tempo de execução e o custo computacional. As métricas de ocupação são calculadas e agregadas conforme necessário, 
fornecendo uma visão consolidada da situação dos leitos. 

-- models/facts/fact_ocupacao_leitos.sql 
{{ config( 
materialized='incremental', 
unique_key=['data_notificacao', 'cnes'], 
on_schema_change='fail' 
) }} 
SELECT 
data_notificacao, 
cnes, 
SUM(ocupacao_suspeito_cli) AS total_ocupacao_suspeito_cli, 
SUM(ocupacao_suspeito_uti) AS total_ocupacao_suspeito_uti, 
SUM(ocupacao_confirmado_cli) AS total_ocupacao_confirmado_cli, 
SUM(ocupacao_confirmado_uti) AS total_ocupacao_confirmado_uti, 
SUM(ocupacao_covid_uti) AS total_ocupacao_covid_uti, 
SUM(ocupacao_covid_cli) AS total_ocupacao_covid_cli, 
SUM(ocupacao_hospitalar_uti) AS total_ocupacao_hospitalar_uti, 
SUM(ocupacao_hospitalar_cli) AS total_ocupacao_hospitalar_cli, 
SUM(saida_suspeita_obitos) AS total_saida_suspeita_obitos, 
SUM(saida_suspeita_altas) AS total_saida_suspeita_altas, 
SUM(saida_confirmada_obitos) AS total_saida_confirmada_obitos, 
SUM(saida_confirmada_altas) AS total_saida_confirmada_altas 
FROM {{ ref('int_leitos_ocupacao_unificado') }} 
{% if is_incremental() %} 
-- Este bloco só será executado em execuções incrementais 
WHERE data_notificacao > (SELECT MAX(data_notificacao) FROM {{ this }}) 
{% endif %} 
GROUP BY 1, 2 

6.9. Testes de Qualidade de Dados ( tests/ ) 

Os testes de qualidade de dados são essenciais para garantir a confiabilidade e a integridade das informações transformadas. O dbt permite a criação de testes genéricos (como not_null , unique , accepted_values ) e testes personalizados. O arquivo schema.yml é utilizado para definir testes para colunas específicas dos modelos, enquanto arquivos .sql separados podem ser usados para testes mais complexos. O teste
test_no_future_dates.sql é um exemplo de teste personalizado que verifica se não há datas futuras nos dados, garantindo a validade temporal. A execução regular desses testes é uma parte fundamental da governança de dados. 

# tests/schema.yml 

version: 2 
models: 
- name: fact_ocupacao_leitos 
columns: 
- name: data_notificacao 
tests: 
- not_null 
- unique 
- dbt_utils.at_least_one 
- dbt_utils.expression_is_true: 
expression: "data_notificacao <= CURRENT_DATE()" 
name: "no_future_dates" 
- name: cnes 
tests: 
- not_null 
- unique 

# tests/test_no_future_dates.sql (teste personalizado) 

SELECT 
data_notificacao 
FROM {{ ref('fact_ocupacao_leitos') }} 
WHERE data_notificacao > CURRENT_DATE() 

7. Pré-requisitos e Configuração 
Para replicar e executar este projeto, os seguintes pré-requisitos e configurações são necessários: 
Snowflake: Uma conta Snowflake ativa com as permissões necessárias para criar bancos de dados, esquemas, tabelas e stages. 
dbt Core: Instalação do dbt Core e do adaptador Snowflake ( dbt-snowflake ). 
Credenciais Snowflake: Configuração do perfil de conexão do Snowflake no arquivo profiles.yml do dbt, incluindo account , user , password , role , warehouse , database e schema . 
Dados de Origem: Os arquivos CSV de dados brutos (ocupação de leitos, municípios, estabelecimentos CNES) devem ser carregados em um estágio (stage) no Snowflake, conforme especificado nos comandos COPY INTO . 

8. Como Executar o Projeto 

Siga os passos abaixo para executar o projeto dbt e transformar os dados: 

8.1. Configurar o profiles.yml : Certifique-se de que seu arquivo profiles.yml (localizado geralmente em ~/.dbt/ ) esteja configurado corretamente para se conectar ao seu ambiente Snowflake. 
yaml covid19: target: dev outputs: dev: type: snowflake account: <SUA_CONTA_SNOWFLAKE> user: <SEU_USUARIO> password: <SUA_SENHA> role: PC_DBT_ROLE # Ou a role que você configurou warehouse: <SEU_WAREHOUSE> database: COVID19 schema: DBT_DEV # Ou o esquema de desenvolvimento threads: 4 client_session_keep_alive: False 

8.2. Criar Tabelas RAW e Carregar Dados: Execute os scripts SQL de criação de tabelas e carregamento de dados ( CREATE OR REPLACE TABLE e COPY INTO ) diretamente no Snowflake. Certifique-se de que os arquivos CSV estejam no estágio correto.

8.3. Conceder Privilégios: Execute os comandos GRANT SELECT no Snowflake para garantir que a role do dbt tenha permissão para ler as tabelas RAW. 

8.4. Instalar Dependências (se houver): Se o projeto tiver dependências de pacotes dbt, execute: bash dbt deps 

8.5. Executar Modelos dbt: Navegue até o diretório raiz do projeto dbt no seu terminal e execute: 
bash dbt run Este comando executará todos os modelos do projeto na ordem correta de dependência, criando as views e tabelas nas camadas SILVER e GOLD . 

8.6. Executar Testes dbt: Para verificar a qualidade dos dados e a integridade das transformações, execute: bash dbt test Este comando executará todos os testes definidos no projeto, reportando quaisquer falhas. 

8.7. Gerar Documentação dbt: Para gerar a documentação interativa do projeto (que pode ser acessada via navegador), execute: 
bash dbt docs generate dbt docs serve O comando dbt docs serve iniciará um servidor web local que hospeda a documentação. O link para a documentação online do dbt deste projeto é: https://kk400.us1.dbt.com/accounts/70471823483193/develop/70471824046419/docs/index.html#!/overview 

9. Decisões de Design e Justificativas 

As decisões de design tomadas neste projeto foram guiadas por princípios de robustez, escalabilidade, manutenibilidade e clareza. Cada escolha, desde a arquitetura em camadas até a materialização dos modelos dbt, foi feita com o objetivo de construir um pipeline de dados eficiente e confiável para a análise de ocupação de leitos. 

9.1. Arquitetura em Camadas (Bronze, Silver, Gold) 

A adoção de uma arquitetura em três camadas (Bronze, Silver, Gold) é uma decisão fundamental que visa: 
Separação de Preocupações: Cada camada tem um propósito bem definido. 

A camada Bronze foca na ingestão e padronização mínima, a Silver no enriquecimento e integração, e a Gold na modelagem para consumo. Isso evita que transformações complexas sejam misturadas com a ingestão de dados brutos, tornando o pipeline mais fácil de entender, depurar e manter.

Reusabilidade: Modelos intermediários na camada Silver podem ser reutilizados por múltiplos modelos de consumo na camada Gold, evitando duplicação de lógica e garantindo consistência. Por exemplo, o modelo int_leitos_ocupacao_unificado serve como base para diversas análises. 

Rastreabilidade e Auditoria: A preservação dos dados brutos na camada Bronze permite a rastreabilidade completa das transformações. Em caso de discrepâncias ou necessidade de auditoria, é possível voltar à fonte original dos dados. 

Performance e Custo: A materialização diferenciada em cada camada (views na Bronze para flexibilidade, tabelas na Silver e Gold para performance) otimiza o uso dos recursos do Snowflake. Views são mais flexíveis para dados brutos que mudam frequentemente, enquanto tabelas materializadas garantem consultas rápidas para dados mais estáveis e transformados. 

9.2. Materialização Incremental para Fatos 

A escolha da materialização incremental para o modelo de fatos ( fact_ocupacao_leitos ) é crucial para a eficiência do pipeline, especialmente em cenários com grandes volumes de dados e atualizações contínuas. As justificativas incluem: 

Redução de Tempo de Execução: Em vez de reconstruir a tabela de fatos inteira a cada execução, apenas os novos dados são processados e adicionados. Isso economiza tempo e recursos computacionais, tornando o pipeline mais ágil. 

Otimização de Custos: Menos processamento significa menor consumo de créditos no Snowflake, resultando em economia de custos operacionais. 

Frescor dos Dados: Permite que os dados sejam atualizados com maior frequência, fornecendo insights mais recentes para a tomada de decisões. 

9.3. Uso de COALESCE e TRIM nos Modelos de Staging 

A aplicação de funções como COALESCE (para tratar valores nulos) e TRIM (para remover espaços em branco) nos modelos de staging ( stg_leito_ocupacao_XXXX.sql ) é uma decisão de design para garantir a qualidade dos dados desde as primeiras etapas: 

Consistência dos Dados: Garante que os valores nulos sejam tratados de forma consistente (neste caso, substituídos por zero para métricas numéricas), evitando erros em cálculos posteriores. O TRIM remove espaços indesejados que poderiam causar problemas em junções ou filtros. 

Facilitação de Análises: Dados limpos e padronizados são mais fáceis de analisar e integrar com outras fontes, reduzindo a necessidade de transformações adicionais nas camadas superiores. 

9.4. Testes de Qualidade de Dados com dbt Tests 

A inclusão de testes de qualidade de dados, tanto genéricos quanto personalizados, é uma prática essencial para a confiabilidade do projeto. A justificativa para esta decisão é: 

Garantia de Integridade: Testes como not_null e unique asseguram que as chaves primárias e colunas críticas mantenham sua integridade. O teste no_future_dates é um exemplo de validação de regras de negócio específicas. 

Detecção Precoce de Erros: Problemas nos dados ou nas transformações são identificados rapidamente, antes que afetem as análises ou os dashboards. 

Confiança nos Dados: A automação dos testes aumenta a confiança dos usuários nos dados, pois sabem que eles foram validados em cada etapa do pipeline.

Manutenibilidade: Ao fazer alterações no código ou nos dados de origem, os testes fornecem uma rede de segurança, alertando sobre quaisquer regressões ou introdução de novos erros. 

9.5. Convenções de Nomenclatura e Organização de Pastas 

Aderir às convenções de nomenclatura ( stg_ , int_ , dim_ , fact_ ) e à organização de pastas ( staging/ , intermediate/ , dimensions/ , facts/ ) é uma decisão que promove: 

Clareza e Compreensão: Facilita a identificação rápida do propósito e da camada de cada modelo, mesmo para novos membros da equipe. 

Manutenibilidade: Um projeto bem organizado é mais fácil de navegar, depurar e estender. 

Colaboração: Reduz a curva de aprendizado para desenvolvedores que trabalham no projeto, promovendo a colaboração eficiente. 

Essas decisões de design, em conjunto, formam a base de um pipeline de dados robusto e eficaz, capaz de entregar dados de alta qualidade para análises críticas de saúde pública. 

10.1. Exemplo de Consulta e Insight Relevante 

Um dos insights mais relevantes que podem ser obtidos a partir dos dados transformados é a identificação de picos de ocupação de leitos de UTI por COVID-19 em diferentes regiões ao longo do tempo. Esta informação é vital para o planejamento de recursos, alocação de equipes médicas e implementação de medidas de contenção da pandemia. 

Considere a seguinte consulta SQL na camada GOLD, que busca a média diária de ocupação de leitos de UTI por COVID-19 por estado e mês, para o ano de 2021: 

WITH hospital_metrics AS (
    SELECT
        dc.nm_estabelecimento,
        dot.tipo_leito,
        SUM(fol.saida_confirmada_altas) AS total_altas_confirmadas,
        SUM(fol.saida_confirmada_obitos) AS total_obitos_confirmados,
        SUM(fol.quantidade_leitos_ocupados) AS total_ocupacao
    FROM FACT_OCUPACAO_LEITOS AS fol
    JOIN DIM_CNES AS dc
        ON fol.id_cnes = dc.id_cnes
    JOIN DIM_TEMPO AS dt
        ON fol.id_tempo = dt.id_tempo
    JOIN DIM_OCUPACAO_TIPO AS dot
        ON fol.id_ocupacao_tipo = dot.id_ocupacao_tipo
    WHERE
        dt.ano = 2021
        AND fol.quantidade_leitos_ocupados IS NOT NULL
        AND fol.quantidade_leitos_ocupados > 0
    GROUP BY
        dc.nm_estabelecimento,
        dot.tipo_leito
)
SELECT
    nm_estabelecimento,
    tipo_leito,
    total_altas_confirmadas,
    total_obitos_confirmados,
    total_ocupacao,
    (total_altas_confirmadas * 100.0 / total_ocupacao) AS taxa_alta_percentual,
    (total_obitos_confirmados * 100.0 / total_ocupacao) AS taxa_obito_percentual
FROM hospital_metrics
WHERE
    total_ocupacao > 0
ORDER BY
    taxa_alta_percentual DESC
LIMIT 5;

O código SQL fornecido gera um insight focado na performance e na gravidade dos casos tratados em diferentes estabelecimentos de saúde. Ao contrário do exemplo dado, que foca em picos de ocupação geográfica e temporal, esta consulta se concentra em métricas de resultado hospitalar.

Insight Gerado:

Ao executar a consulta, é possível identificar os 5 principais estabelecimentos de saúde (hospitais) que tiveram as maiores taxas de alta de pacientes em relação à sua ocupação total de leitos em 2021, discriminado por tipo de leito (por exemplo, UTI ou leito clínico). A consulta também fornece as taxas de óbito para cada um desses hospitais.

Este insight permite que gestores de saúde e analistas:

Identifiquem a Eficácia do Tratamento: A alta taxa de altas (taxa_alta_percentual) sugere quais hospitais podem ter protocolos de tratamento mais eficazes, equipes mais especializadas ou uma população de pacientes com menor gravidade. É uma métrica de eficiência no desfecho clínico.

Avaliem a Qualidade do Cuidado: A análise conjunta da taxa de alta e da taxa de óbito oferece um panorama mais completo. Hospitais com alta taxa de alta e baixa taxa de óbito podem ser considerados referências de excelência no tratamento.

Compreendam a Carga de Casos Graves: A taxa de óbito (taxa_obito_percentual) ajuda a entender a gravidade dos casos que um hospital atende. Se um hospital tem uma alta taxa de óbito, isso pode indicar que ele é um centro de referência para casos mais complexos e graves, não necessariamente que o tratamento é ineficaz.

Otimizem o Planejamento e a Capacitação: As informações podem ser usadas para identificar boas práticas em hospitais com alto desempenho e replicá-las em outras unidades. Além disso, hospitais com altas taxas de óbito podem necessitar de mais recursos ou suporte especializado para lidar com a complexidade dos casos.

Monitorem o Desempenho Interno: Cada hospital pode usar esta análise para monitorar seu próprio desempenho ao longo do tempo, comparando-o com o de outras unidades e buscando melhorias contínuas.

11. Inovação Implementada 

Este projeto incorpora diversas inovações e abordagens que o tornam robusto, eficiente e alinhado com as melhores práticas de engenharia de dados. As principais inovações implementadas incluem: 

11.1. Abordagem Data-as-Code com dbt 

A utilização do dbt (data build tool) como pilar central para as transformações de dados representa uma inovação significativa. O dbt permite tratar as transformações SQL como código de software, aplicando princípios de engenharia de software ao pipeline de dados. Isso inclui: 

Versionamento de Código: Todas as transformações são versionadas em um sistema de controle de versão (e.g., Git), facilitando o rastreamento de mudanças, a colaboração e a reversão para versões anteriores, se necessário. 

Testes Automatizados: A capacidade de definir e executar testes de dados diretamente no dbt garante a qualidade e a integridade dos dados em cada etapa da transformação. Isso reduz a probabilidade de erros e aumenta a confiança nos dados finais. 

Documentação Automática: O dbt gera automaticamente uma documentação interativa do projeto, incluindo o grafo de dependências dos modelos, descrições de colunas e resultados de testes. Isso melhora a governança de dados e facilita a compreensão do pipeline por todos os stakeholders. 

Modularização e Reusabilidade: As transformações são divididas em modelos modulares, que podem ser reutilizados e combinados para construir pipelines complexos de forma eficiente. Isso promove a consistência e reduz a duplicação de código. 

11.2. Modelagem Dimensional Otimizada para Análise 

A implementação de um modelo dimensional (tabelas de fatos e dimensões) na camada GOLD é uma inovação que otimiza os dados para consumo analítico. Embora a modelagem dimensional seja uma prática estabelecida, a forma como foi aplicada neste projeto, considerando a natureza dos dados de saúde pública e a necessidade de insights rápidos, é um diferencial: 

Performance de Consulta: A estrutura dimensional minimiza a necessidade de junções complexas em tempo de consulta, resultando em dashboards e relatórios mais rápidos e responsivos. 

Facilidade de Uso: Analistas de negócio podem navegar e consultar os dados de forma intuitiva, sem a necessidade de conhecimento aprofundado da estrutura dos dados brutos. 

Flexibilidade Analítica: Permite a análise de dados sob diversas perspectivas (por tempo, localização, tipo de leito, etc.), facilitando a descoberta de insights e a criação de relatórios ad-hoc. 

11.3. Automação e Orquestração com Snowflake Tasks e Streams (Potencial) 

Embora o README atual mencione apenas COPY INTO para ingestão inicial, a sugestão de utilizar Snowflake Tasks e Streams para automação e orquestração incremental representa uma inovação potencial e uma melhoria significativa para um ambiente de produção: 

Ingestão em Tempo Quase Real: Snowflake Streams podem capturar mudanças nos dados de origem (e.g., novos arquivos CSV chegando em um estágio) e Snowflake Tasks podem ser agendadas para executar o dbt de forma incremental quando novas mudanças são detectadas. Isso permite que o pipeline de dados opere em um modo quase em tempo real, fornecendo dados mais frescos para análise.

Redução de Latência: A automação da ingestão e transformação minimiza a latência entre a chegada dos dados brutos e sua disponibilidade na camada de consumo. 

Eficiência Operacional: Elimina a necessidade de intervenção manual para iniciar o pipeline, liberando recursos da equipe de engenharia de dados. 

11.4. Tratamento Abrangente de Dados Históricos 

A inclusão e o tratamento de dados de múltiplos anos (2020, 2021, 2022) de forma consolidada é uma inovação que permite análises de séries temporais e comparações entre diferentes períodos da pandemia. A unificação dos dados de staging ( stg_leito_ocupacao_consolidado.sql ) é um exemplo de como essa inovação foi implementada, fornecendo uma visão holística da evolução da ocupação de leitos ao longo do tempo. 

Essas inovações, combinadas com as melhores práticas de engenharia de dados, garantem que o projeto não apenas atenda aos requisitos atuais de análise, mas também seja escalável e adaptável a futuras necessidades e desafios no campo da saúde pública.

