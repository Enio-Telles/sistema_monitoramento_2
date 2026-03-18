WITH PARAMETROS AS (
    SELECT
        :CNPJ AS cnpj_filtro,
        NVL(
            TO_DATE(:data_limite_processamento, 'DD/MM/YYYY'),
            TRUNC(SYSDATE)
        ) AS dt_corte
    FROM dual
),
TODOS_ARQUIVOS_VALIDOS AS (
    SELECT
        dm.REG_0000_ID AS reg_0000_id,
        dm.DA_INICIO_ARQUIVO AS dt_ini,
        dm.DA_ENTREGA_ARQUIVO AS data_entrega,
        dm.IN_CODIGO_FINALIDADE AS cod_fin_efd,
        ROW_NUMBER() OVER (
            PARTITION BY dm.CO_CNPJ_CPF_DECLARANTE, dm.DA_INICIO_ARQUIVO
            ORDER BY dm.DA_ENTREGA_ARQUIVO DESC, dm.REG_0000_ID DESC
        ) AS rn
    FROM BI.DM_EFD_ARQUIVO_VALIDO dm
    CROSS JOIN PARAMETROS p
    WHERE dm.CO_CNPJ_CPF_DECLARANTE = p.cnpj_filtro
      AND dm.DA_ENTREGA_ARQUIVO <= p.dt_corte
),
ARQUIVOS_VALIDOS_FINAIS AS (
    SELECT
        reg_0000_id,
        dt_ini,
        data_entrega,
        cod_fin_efd
    FROM TODOS_ARQUIVOS_VALIDOS
    WHERE rn = 1
),
BASE AS (
    SELECT
        arq.dt_ini AS periodo_efd_dt,
        arq.data_entrega,
        arq.cod_fin_efd,
        CASE c100.ind_oper
            WHEN '0' THEN '0 - Entrada'
            WHEN '1' THEN '1 - Saída'
        END AS ind_oper_desc,
        CASE c100.ind_emit
            WHEN '0' THEN '0 - Emissão própria'
            WHEN '1' THEN '1 - Terceiros'
        END AS ind_emit_desc,
        c100.cod_part,
        c100.cod_mod,
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
        END AS descricao_cod_sit,
        c100.ser,
        c100.num_doc,
        c100.chv_nfe,
        CASE
            WHEN c100.dt_doc IS NOT NULL
             AND REGEXP_LIKE(c100.dt_doc, '^\d{8}$')
            THEN TO_DATE(c100.dt_doc, 'DDMMYYYY')
        END AS dt_doc,
        CASE
            WHEN c100.dt_e_s IS NOT NULL
             AND REGEXP_LIKE(c100.dt_e_s, '^\d{8}$')
            THEN TO_DATE(c100.dt_e_s, 'DDMMYYYY')
        END AS dt_e_s,
        c100.vl_doc,
        CASE c100.ind_pgto
            WHEN '0' THEN '0 - a vista'
            WHEN '1' THEN '1 - a prazo'
            WHEN '2' THEN '2 - outros'
            WHEN '9' THEN '9 - Sem pagamento'
        END AS ind_pgto_desc,
        c100.vl_desc,
        c100.vl_abat_nt,
        c100.vl_merc,
        CASE c100.ind_frt
            WHEN '0' THEN '0 - Emitente'
            WHEN '1' THEN '1 - Destinatário'
            WHEN '2' THEN '2 - Terceiros'
            WHEN '9' THEN '9 - Sem frete'
        END AS ind_frt_por_conta_de,
        c100.vl_frt,
        c100.vl_seg,
        c100.vl_out_da,
        c100.vl_bc_icms,
        c100.vl_icms,
        c100.vl_bc_icms_st,
        c100.vl_icms_st,
        c100.vl_ipi,
        c100.vl_pis,
        c100.vl_cofins,
        c100.vl_pis_st,
        c100.vl_cofins_st
    FROM SPED.REG_C100 c100
    INNER JOIN ARQUIVOS_VALIDOS_FINAIS arq
        ON c100.REG_0000_ID = arq.REG_0000_ID
)
SELECT
    TO_CHAR(periodo_efd_dt, 'YYYY/MM') AS periodo_efd,
    data_entrega AS data_entrega_efd_periodo,
    cod_fin_efd,
    ind_oper_desc,
    ind_emit_desc,
    cod_part,
    cod_mod,
    cod_sit,
    descricao_cod_sit,
    ser,
    num_doc,
    chv_nfe,
    dt_doc,
    dt_e_s,
    vl_doc,
    ind_pgto_desc,
    vl_desc,
    vl_abat_nt,
    vl_merc,
    ind_frt_por_conta_de,
    vl_frt,
    vl_seg,
    vl_out_da,
    vl_bc_icms,
    vl_icms,
    vl_bc_icms_st,
    vl_icms_st,
    vl_ipi,
    vl_pis,
    vl_cofins,
    vl_pis_st,
    vl_cofins_st
FROM BASE
ORDER BY dt_doc, ser, num_doc;