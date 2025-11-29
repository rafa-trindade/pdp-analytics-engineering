WITH src AS (
    SELECT
        apartamento_id,
        apartamento_numero
    FROM {{ source('staging', 'stg_apartamento') }}
)

SELECT * FROM src