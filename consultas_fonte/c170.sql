WITH
    PARAMETROS AS (
        SELECT
            :CNPJ AS cnpj_filtro,
            NVL(
                TO_DATE(:data_limite_processamento, 'DD/MM/YYYY'),
                TRUNC(SYSDATE)
            ) AS dt_corte
        FROM dual
    ),

    ARQUIVOS_VALIDOS AS (
        SELECT reg_0000_id, dt_ini
        FROM (
            SELECT
                r.id AS reg_0000_id,
                r.dt_ini,
                ROW_NUMBER() OVER (
                    PARTITION BY r.cnpj, r.dt_ini
                    ORDER BY r.data_entrega DESC, r.id DESC
                ) AS rn
            FROM sped.reg_0000 r
            CROSS JOIN PARAMETROS p
            WHERE r.cnpj = p.cnpj_filtro
              AND r.data_entrega <= p.dt_corte
        )
        WHERE rn = 1
    ),

    DADOS_C100 AS (
        SELECT
            c100.id,
            c100.reg_0000_id,
            TRIM(c100.chv_nfe) AS chv_nfe,
            c100.cod_sit,
            CASE c100.cod_sit
                WHEN '00' THEN 'Documento regular'
                WHEN '01' THEN 'Escrituração extemporânea de documento regular'
                WHEN '02' THEN 'Documento cancelado'
                WHEN '03' THEN 'Escrituração extemporânea de documento cancelado'
                WHEN '04' THEN 'NF-e, NFC-e ou CT-e - denegado'
                WHEN '05' THEN 'NF-e, NFC-e ou CT-e - Numeração inutilizada'
                WHEN '06' THEN 'Documento Fiscal Complementar'
                WHEN '07' THEN 'Escrituração extemporânea de documento complementar'
                WHEN '08' THEN 'Documento Fiscal emitido com base em Regime Especial ou Norma Específica'
                ELSE 'Código desconhecido'
            END AS cod_sit_desc,
            c100.ind_emit,
            CASE c100.ind_emit
                WHEN '0' THEN '0 - Emissão própria'
                WHEN '1' THEN '1 - Terceiros'
                ELSE 'Não informado'
            END AS ind_emit_desc,
            c100.ind_oper,
            CASE c100.ind_oper
                WHEN '0' THEN '0 - Entrada'
                WHEN '1' THEN '1 - Saída'
                ELSE 'Não informado'
            END AS ind_oper_desc,
            c100.num_doc,
            CASE
                WHEN REGEXP_LIKE(c100.dt_doc, '^\d{8}$')
                THEN TO_DATE(c100.dt_doc, 'DDMMYYYY')
            END AS dt_doc
        FROM sped.reg_c100 c100
        JOIN ARQUIVOS_VALIDOS arq
          ON arq.reg_0000_id = c100.reg_0000_id
        -- WHERE c100.cod_sit IN ('00', '01')
    ),

    DADOS_C170 AS (
        SELECT
            c170.reg_c100_id,
            c170.reg_0000_id,
            arq.dt_ini,
            c170.num_item,
            c170.cod_item,
            c170.descr_compl,
            c170.cfop,
            c170.cst_icms,
            c170.qtd,
            c170.unid,
            c170.vl_item,
            c170.vl_icms,
            c170.vl_bc_icms,
            c170.aliq_icms,
            c170.vl_bc_icms_st,
            c170.vl_icms_st,
            c170.aliq_st
        FROM sped.reg_c170 c170
        JOIN DADOS_C100 c100
          ON c100.id = c170.reg_c100_id
         AND c100.reg_0000_id = c170.reg_0000_id
        JOIN ARQUIVOS_VALIDOS arq
          ON arq.reg_0000_id = c170.reg_0000_id
    ),

    DADOS_0200 AS (
        SELECT
            r200.reg_0000_id,
            r200.cod_item,
            r200.cod_barra,
            r200.cod_ncm,
            r200.cest,
            r200.tipo_item,
            r200.descr_item
        FROM sped.reg_0200 r200
        JOIN ARQUIVOS_VALIDOS arq
          ON arq.reg_0000_id = r200.reg_0000_id
    )

SELECT
    TO_CHAR(c170.dt_ini, 'YYYY/MM') AS periodo_efd,
    c100.chv_nfe,
    c100.cod_sit,
    c100.cod_sit_desc,
    c100.ind_emit,
    c100.ind_emit_desc,
    c100.ind_oper,
    c100.ind_oper_desc,
    c100.num_doc,
    c100.dt_doc,
    c170.num_item,
    c170.cod_item,
    r200.cod_barra,
    r200.cod_ncm,
    r200.cest,
    r200.tipo_item,
    r200.descr_item,
    c170.descr_compl,
    c170.cfop,
    c170.cst_icms,
    NVL(c170.qtd, 0) AS qtd,
    c170.unid,
    c170.vl_item,
    NVL(c170.vl_icms, 0) AS vl_icms,
    NVL(c170.vl_bc_icms, 0) AS vl_bc_icms,
    c170.aliq_icms,
    NVL(c170.vl_bc_icms_st, 0) AS vl_bc_icms_st,
    NVL(c170.vl_icms_st, 0) AS vl_icms_st,
    c170.aliq_st
FROM DADOS_C170 c170
JOIN DADOS_C100 c100
  ON c100.id = c170.reg_c100_id
 AND c100.reg_0000_id = c170.reg_0000_id
LEFT JOIN DADOS_0200 r200
  ON r200.reg_0000_id = c170.reg_0000_id
 AND r200.cod_item = c170.cod_item
ORDER BY
    c170.dt_ini,
    c100.num_doc,
    c170.num_item;