WITH base AS (
    SELECT
        fh.data_hospedagem_key,
        d.data,
        fh.hospedagem_valor,
        fh.total_consumo,
        fh.hospedagem_qtd_pessoas,
        d.feriado,
        d.nome_feriado,
        d.fim_de_semana,
        d.nome_dia_semana AS dia_semana
    FROM {{ source('core', 'fact_hospedagem') }} fh
    JOIN {{ source('core', 'dim_data') }} d
        ON fh.data_hospedagem_key = d.chave_data
)

, agregados AS (
    SELECT
        data,
        SUM(hospedagem_valor) AS hospedagem,
        SUM(total_consumo) AS consumo,
        SUM(hospedagem_qtd_pessoas) AS quantidade_hospedes,
        CASE 
            WHEN BOOL_OR(feriado) THEN MAX(nome_feriado)
            WHEN BOOL_OR(fim_de_semana) THEN 'FDS'
            ELSE '-'
        END AS observacao,
        MAX(dia_semana) AS dia_semana
    FROM base
    GROUP BY data
)

SELECT
    data,
    hospedagem,
    consumo,
    dia_semana,
    observacao,
    quantidade_hospedes,
    (hospedagem + consumo) AS total,
    ROUND(
        CASE 
            WHEN quantidade_hospedes = 0 THEN 0
            ELSE hospedagem / quantidade_hospedes
        END
    , 2) AS apt
FROM agregados
ORDER BY data ASC