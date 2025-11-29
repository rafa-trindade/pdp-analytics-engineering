WITH src AS (
    SELECT
        empresa_id,
        empresa_nome
    FROM {{ source('staging', 'stg_empresa') }}
)

SELECT * FROM src
