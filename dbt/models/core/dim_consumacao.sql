WITH base AS (
    SELECT
        c.consumacao_id,
        c.produto_id,
        c.data_consumacao,
        p.produto_nome,
        c.quantidade_produto::INTEGER AS quantidade_produto,
        c.valor_produto::NUMERIC AS valor_produto,
        c.valor_consumacao::NUMERIC AS valor_consumacao,
        c.hospedagem_id
    FROM {{ source('staging', 'stg_consumacao') }} c
    LEFT JOIN {{ ref('dim_produto') }} p
        ON c.produto_id = p.produto_id
)

SELECT * FROM base