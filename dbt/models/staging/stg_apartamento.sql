-- models/staging/stg_apartamento.sql
WITH raw AS (
    SELECT
        "Id" as apartamento_id,
        "Descricao" as apartamento_numero
    FROM {{ source('raw', 'clsapartamento') }}
)

SELECT * FROM raw
