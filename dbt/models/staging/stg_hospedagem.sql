-- models/staging/stg_hospedagem.sql
WITH raw AS (
    SELECT
        "Id" as hospedagem_id,
        "Cliente_Id" as cliente_id,
        "Apartamento_Id" as apartamento_id,
        "DataEntrada" as data_entrada,
        "DataSaida" as data_saida,
        "QuantidadeDiaria" as hospedagem_qtd_diarias,
        "ValorTotal" as hospedagem_valor,
        "QuantidadePessoas" as hospedagem_qtd_pessoas
    FROM {{ source('raw', 'clshospedagem') }}
)

SELECT * FROM raw
