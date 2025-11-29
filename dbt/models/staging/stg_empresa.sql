-- models/staging/stg_empresa.sql
WITH raw AS (
    SELECT
        "Id" as empresa_id,
        "NomeFantasia" as empresa_nome
    FROM {{ source('raw', 'clsempresa') }}
)

SELECT * FROM raw
