WITH base AS (
    SELECT
        c.cliente_id,
        c.empresa_id,
        c.cliente_data_cadastro,
        c.cliente_nome,
        e.empresa_nome
    FROM {{ source('staging', 'stg_cliente') }} c
    LEFT JOIN {{ ref('dim_empresa') }} e
        ON c.empresa_id = e.empresa_id
)

SELECT * FROM base

