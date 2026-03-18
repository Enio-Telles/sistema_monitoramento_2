WITH
    PARAMETROS AS (
        SELECT
:CNPJ AS cnpj_filtro,
            NVL (
                TO_DATE(
:data_limite_processamento,
                    'DD/MM/YYYY'
                ),
                TRUNC(SYSDATE)
            ) AS dt_corte
        FROM dual
    ),
    TODOS_ARQUIVOS_VALIDOS AS (
        SELECT
            dm.REG_0000_ID AS reg_0000_id,
            dm.DA_INICIO_ARQUIVO AS dt_ini,
            dm.DA_FINAL_ARQUIVO AS dt_fin,
            dm.CO_CNPJ_CPF_DECLARANTE AS cnpj,
            dm.DA_ENTREGA_ARQUIVO AS data_entrega,
            dm.IN_CODIGO_FINALIDADE AS cod_fin_efd,
            p.dt_corte,
            ROW_NUMBER() OVER (
                PARTITION BY
                    dm.CO_CNPJ_CPF_DECLARANTE,
                    dm.DA_INICIO_ARQUIVO
                ORDER BY dm.DA_ENTREGA_ARQUIVO DESC
            ) AS rn
        FROM BI.DM_EFD_ARQUIVO_VALIDO dm
            JOIN PARAMETROS p ON dm.CO_CNPJ_CPF_DECLARANTE = p.cnpj_filtro
        WHERE
            dm.DA_ENTREGA_ARQUIVO <= p.dt_corte
    ),
    ARQUIVOS_VALIDOS_FINAIS AS (
        SELECT *
        FROM TODOS_ARQUIVOS_VALIDOS
        WHERE
            rn = 1
    )

SELECT
    TO_CHAR(arq.dt_ini, 'YYYY/MM') AS periodo_efd,
    arq.data_entrega AS Data_entrega_efd_periodo,
    arq.cod_fin_efd,
    CASE arq.cod_fin_efd
        WHEN '0' THEN '0 - Original'
        WHEN '1' THEN '1 - Substituto'
        ELSE TO_CHAR(arq.cod_fin_efd)
    END AS finalidade_efd,
    c100.chv_nfe AS chave_saida,
    c100.num_doc AS num_nf_saida,
    c100.dt_doc AS dt_doc_saida,
    c100.dt_e_s AS dt_e_s_saida,
    c170.cod_item AS cod_item,
    c170.descr_compl AS descricao_item,
    c170.num_item AS num_item_saida,
    c170.cfop AS cfop_saida,
    c170.unid AS unid_saida,
    c170.qtd AS qtd_item_saida,
    c170.vl_item AS vl_total_item,
    c176.cod_mot_res AS cod_mot_res,
    CASE C176.cod_mot_res
        WHEN '1' THEN '1 - Saida para outra UF'
        WHEN '2' THEN '2 - Isencao ou nao incidencia'
        WHEN '3' THEN '3 - Perda ou deterioracao'
        WHEN '4' THEN '4 - Furto ou roubo'
        WHEN '5' THEN '5 - Exportacao'
        WHEN '6' THEN '6 - Venda interna p/ Simples Nacional'
        WHEN '9' THEN '9 - Outros'
        ELSE C176.cod_mot_res
    END AS descricao_motivo_ressarcimento,
    c176.chave_nfe_ult AS chave_nfe_ultima_entrada,
    c176.num_item_ult_e AS c176_num_item_ult_e_declarado,
    CASE
        WHEN c176.dt_ult_e IS NOT NULL
        AND REGEXP_LIKE (c176.dt_ult_e, '^\d{8}$') THEN TO_DATE(c176.dt_ult_e, 'DDMMYYYY')
        ELSE NULL
    END AS dt_ultima_entrada,
    c176.vl_unit_ult_e AS vl_unit_bc_st_entrada,
    c176.vl_unit_icms_ult_e AS vl_unit_icms_proprio_entrada,
    c176.vl_unit_res AS vl_unit_ressarcimento_st,
    
    -- Cálculos de Apoio (Conforme regras de ressarcimento)
    (
        NVL(c170.qtd, 0) * NVL(c176.vl_unit_icms_ult_e, 0)
    ) AS vl_ressarc_credito_proprio,
    
    (
        NVL(c170.qtd, 0) * NVL(c176.vl_unit_res, 0)
    ) AS vl_ressarc_st_retido,
    
    CASE
        WHEN NVL(c170.vl_icms, 0) > 0 THEN (c170.qtd * c176.vl_unit_res) + (c170.qtd * c176.vl_unit_icms_ult_e)
        ELSE (c170.qtd * c176.vl_unit_res)
    END AS vr_total_ressarcimento
FROM
    sped.reg_c176 c176
    INNER JOIN ARQUIVOS_VALIDOS_FINAIS arq ON c176.reg_0000_id = arq.reg_0000_id
    INNER JOIN sped.reg_c100 c100 ON c176.reg_c100_id = c100.id
    INNER JOIN sped.reg_c170 c170 ON c176.reg_c170_id = c170.id