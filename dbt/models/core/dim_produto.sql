WITH src AS (
    SELECT
        produto_id,
        produto_nome,
        produto_valor
    FROM {{ source('staging', 'stg_produto') }}
)

SELECT * FROM src
