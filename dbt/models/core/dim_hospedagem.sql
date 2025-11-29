WITH src AS (
    SELECT
        h.hospedagem_id,
        h.cliente_id,
        cl.empresa_id,
        h.apartamento_id,
        cl.cliente_nome,
        e.empresa_nome,
        ap.apartamento_numero,
        h.data_entrada,
        h.data_saida,
        h.hospedagem_qtd_diarias,
        h.hospedagem_valor,
        h.hospedagem_qtd_pessoas
    FROM {{ source('staging', 'stg_hospedagem') }} h
    LEFT JOIN {{ ref('dim_cliente') }} cl
        ON h.cliente_id = cl.cliente_id
    LEFT JOIN {{ ref('dim_empresa') }} e
        ON cl.empresa_id = e.empresa_id
    LEFT JOIN {{ ref('dim_apartamento') }} ap
        ON h.apartamento_id = ap.apartamento_id
)

SELECT * FROM src
