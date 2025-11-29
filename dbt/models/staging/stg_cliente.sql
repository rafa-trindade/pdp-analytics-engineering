-- models/staging/stg_cliente.sql
WITH raw AS (
    SELECT
        "Id" as cliente_id,
        "Nome" as cliente_nome,
        "DataCadastro" as cliente_data_cadastro,
        "Empresa_Id" as empresa_id
    FROM {{ source('raw', 'clscliente') }}
)

SELECT * FROM raw