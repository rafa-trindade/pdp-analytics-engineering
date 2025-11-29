WITH base AS (
    SELECT
        c.consumacao_id,
        cl.cliente_id,
        e.empresa_id,
        p.produto_id,
        d.chave_data AS data_consumacao_key,
        c.quantidade_produto::INTEGER AS quantidade_produto,
        c.valor_produto::NUMERIC AS valor_produto,
        c.valor_consumacao::NUMERIC AS valor_consumacao
    FROM {{ ref('dim_consumacao') }} c
    LEFT JOIN {{ ref('dim_hospedagem') }} h
        ON c.hospedagem_id = h.hospedagem_id
    LEFT JOIN {{ ref('dim_cliente') }} cl
        ON h.cliente_id = cl.cliente_id
    LEFT JOIN {{ ref('dim_empresa') }} e
        ON cl.empresa_id = e.empresa_id
    LEFT JOIN {{ ref('dim_produto') }} p
        ON c.produto_id = p.produto_id
    LEFT JOIN {{ ref('dim_data') }} d
        ON CAST(c.data_consumacao AS DATE) = d.data
)

SELECT * FROM base
