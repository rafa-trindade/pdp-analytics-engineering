-- models/staging/stg_consumacao.sql
WITH raw AS (
    SELECT
        "Id" as consumacao_id,
        "Hospedagem_Id" as hospedagem_id,
        "Produto_Id" as produto_id,
        "DataConsumacao" as data_consumacao,
        "QtdProduto" as quantidade_produto,
        "ValorProduto" as valor_produto,
        "ValorTotalConsumo" as valor_consumacao
    FROM {{ source('raw', 'clsconsumacao') }}
)

SELECT * FROM raw
