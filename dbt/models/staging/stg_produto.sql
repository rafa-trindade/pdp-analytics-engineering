-- models/staging/stg_produto.sql
WITH raw AS (
    SELECT
        "Id" as produto_id,
        "Descricao" as produto_nome,
        "PrecoVenda" as produto_valor
    FROM {{ source('raw', 'clsproduto') }}
)

SELECT * FROM raw
