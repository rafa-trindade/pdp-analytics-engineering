WITH base AS (
    SELECT
        h.hospedagem_id,
        h.cliente_id,
        h.apartamento_id,
        cl.empresa_id,
        d_entrada.chave_data AS data_hospedagem_key,
        COALESCE(CAST(h.hospedagem_qtd_diarias AS NUMERIC), 0) AS hospedagem_qtd_diarias,
        COALESCE(CAST(h.hospedagem_qtd_pessoas AS NUMERIC), 0) AS hospedagem_qtd_pessoas,
        COALESCE(CAST(h.hospedagem_valor AS NUMERIC), 0) AS hospedagem_valor,
        COALESCE(cons.total_consumo, 0) AS total_consumo,
        (COALESCE(CAST(h.hospedagem_valor AS NUMERIC), 0) + COALESCE(cons.total_consumo, 0)) AS valor_total,
        COALESCE(cons.total_qtd_produtos, 0)::INTEGER AS total_qtd_produtos
    FROM {{ ref('dim_hospedagem') }} h
    LEFT JOIN {{ ref('dim_cliente') }} cl
        ON h.cliente_id = cl.cliente_id
    LEFT JOIN {{ ref('dim_empresa') }} e
        ON cl.empresa_id = e.empresa_id
    LEFT JOIN {{ ref('dim_apartamento') }} ap
        ON h.apartamento_id = ap.apartamento_id
    LEFT JOIN (
        SELECT
            hospedagem_id,
            SUM(COALESCE(CAST(valor_consumacao AS NUMERIC), 0)) AS total_consumo,
            SUM(COALESCE(CAST(quantidade_produto AS NUMERIC), 0))::INTEGER AS total_qtd_produtos
        FROM {{ ref('dim_consumacao') }}
        GROUP BY hospedagem_id
    ) cons
        ON h.hospedagem_id = cons.hospedagem_id
    LEFT JOIN {{ ref('dim_data') }} d_entrada
        ON CAST(h.data_entrada AS DATE) = d_entrada.data
)

SELECT * FROM base
