WITH base AS (
    SELECT
        dd.data AS data,
        dd.tipo,
        dd.valor
    FROM {{ source('core', 'dim_despesas') }} dd
)

, agregados AS (
    SELECT
        data,
        tipo AS tipo_despesa,
        SUM(valor) AS total_despesa
    FROM base
    GROUP BY data, tipo_despesa
    ORDER BY data, tipo_despesa
)

SELECT
    data,
    tipo_despesa,
    total_despesa
FROM agregados