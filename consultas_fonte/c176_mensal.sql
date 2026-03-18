WITH
    PARAMETROS AS (
        SELECT
            :CNPJ AS cnpj_filtro,
            NVL(TO_DATE(:data_inicial, 'DD/MM/YYYY'), DATE '1900-01-01') AS dt_ini_filtro,
            NVL(TO_DATE(:data_final, 'DD/MM/YYYY'), TRUNC(SYSDATE)) AS dt_fim_filtro,
            NVL(TO_DATE(:data_limite_processamento, 'DD/MM/YYYY'), TRUNC(SYSDATE)) AS dt_corte
        FROM dual
    ),

    ARQUIVOS_RANKING AS (
        SELECT
            r.id AS reg_0000_id,
            r.cnpj,
            r.cod_fin AS cod_fin_efd,
            r.dt_ini,
            r.data_entrega,
            ROW_NUMBER() OVER (
                PARTITION BY r.cnpj, r.dt_ini
                ORDER BY r.data_entrega DESC, r.id DESC
            ) AS rn
        FROM sped.reg_0000 r
        CROSS JOIN PARAMETROS p
        WHERE r.cnpj = p.cnpj_filtro
          AND r.data_entrega <= p.dt_corte
          AND r.dt_ini BETWEEN p.dt_ini_filtro AND p.dt_fim_filtro
    ),

    ARQUIVOS_VALIDOS AS (
        SELECT
            reg_0000_id,
            cnpj,
            cod_fin_efd,
            dt_ini
        FROM ARQUIVOS_RANKING
        WHERE rn = 1
    ),

    RESUMO_RESSARCIMENTO AS (
        SELECT
            arq.cnpj,
            TRUNC(arq.dt_ini, 'MM') AS mes_referencia,
            COUNT(c176.id) AS qtd_itens_analisados,
            SUM(NVL(c170.qtd, 0) * NVL(c176.vl_unit_icms_ult_e, 0)) AS vl_ressarc_credito_proprio,
            SUM(NVL(c170.qtd, 0) * NVL(c176.vl_unit_res, 0)) AS vl_ressarc_st_retido
        FROM ARQUIVOS_VALIDOS arq
        INNER JOIN sped.reg_c176 c176
            ON c176.reg_0000_id = arq.reg_0000_id
        INNER JOIN sped.reg_c100 c100
            ON c100.id = c176.reg_c100_id
           AND c100.reg_0000_id = arq.reg_0000_id
        INNER JOIN sped.reg_c170 c170
            ON c170.id = c176.reg_c170_id
           AND c170.reg_0000_id = arq.reg_0000_id
        GROUP BY
            arq.cnpj,
            TRUNC(arq.dt_ini, 'MM')
    ),

    RESUMO_E111 AS (
        SELECT
            arq.cnpj,
            TRUNC(arq.dt_ini, 'MM') AS mes_referencia,
            SUM(CASE
                    WHEN e111.cod_aj_apur IN ('RO020023', 'RO020049')
                    THEN NVL(e111.vl_aj_apur, 0)
                    ELSE 0
                END) AS vl_ajuste_credito_proprio,
            SUM(CASE
                    WHEN e111.cod_aj_apur IN ('RO020022', 'RO020047')
                    THEN NVL(e111.vl_aj_apur, 0)
                    ELSE 0
                END) AS vl_ajuste_st_retido,
            SUM(CASE
                    WHEN e111.cod_aj_apur = 'RO020050'
                    THEN NVL(e111.vl_aj_apur, 0)
                    ELSE 0
                END) AS vl_ajuste_ro020050,
            SUM(CASE
                    WHEN e111.cod_aj_apur = 'RO020048'
                    THEN NVL(e111.vl_aj_apur, 0)
                    ELSE 0
                END) AS vl_ajuste_ro020048
        FROM ARQUIVOS_VALIDOS arq
        INNER JOIN sped.reg_e111 e111
            ON e111.reg_0000_id = arq.reg_0000_id
        GROUP BY
            arq.cnpj,
            TRUNC(arq.dt_ini, 'MM')
    )

SELECT
    COALESCE(res.cnpj, e.cnpj) AS cnpj,
    TO_CHAR(COALESCE(res.mes_referencia, e.mes_referencia), 'MM/YYYY') AS periodo_efd,

    NVL(res.qtd_itens_analisados, 0) AS qtd_itens_analisados_c176,

    NVL(res.vl_ressarc_credito_proprio, 0) AS total_ressarc_credito_proprio,
    NVL(e.vl_ajuste_credito_proprio, 0) AS total_ajuste_credito_proprio_e111,
    NVL(res.vl_ressarc_credito_proprio, 0) - NVL(e.vl_ajuste_credito_proprio, 0) AS diferenca_credito_proprio,

    NVL(res.vl_ressarc_st_retido, 0) AS total_ressarc_st_retido,
    NVL(e.vl_ajuste_st_retido, 0) AS total_ajuste_st_retido_e111,
    NVL(res.vl_ressarc_st_retido, 0) - NVL(e.vl_ajuste_st_retido, 0) AS diferenca_st_retido,

    NVL(e.vl_ajuste_ro020050, 0) AS total_ajuste_ro020050_e111,
    NVL(e.vl_ajuste_ro020048, 0) AS total_ajuste_ro020048_e111

FROM RESUMO_RESSARCIMENTO res
FULL OUTER JOIN RESUMO_E111 e
    ON res.cnpj = e.cnpj
   AND res.mes_referencia = e.mes_referencia
ORDER BY
    COALESCE(res.mes_referencia, e.mes_referencia);