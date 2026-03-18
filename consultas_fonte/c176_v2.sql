WITH PARAMETROS AS (
    SELECT 
        :CNPJ AS cnpj_filtro,
        NVL(TO_DATE(:data_inicial, 'DD/MM/YYYY'), TO_DATE('01/01/1900', 'DD/MM/YYYY')) AS dt_ini_filtro,
        NVL(TO_DATE(:data_final,   'DD/MM/YYYY'), TRUNC(SYSDATE)) AS dt_fim_filtro,
        NVL(TO_DATE(:data_limite_processamento, 'DD/MM/YYYY'), TRUNC(SYSDATE)) AS dt_corte,
        :codigo_item AS cod_item_filtro,
        :chave_acesso AS chave_acesso_filtro
    FROM dual
),

ARQUIVOS_RANKING AS (
    SELECT
        r.id as reg_0000_id,
        r.cnpj,
        r.cod_fin,
        r.dt_ini,
        r.dt_fin,
        r.data_entrega,
        p.dt_corte,
        p.cod_item_filtro,
        p.chave_acesso_filtro,
        ROW_NUMBER() OVER (
            PARTITION BY r.cnpj, r.dt_ini 
            ORDER BY r.data_entrega DESC
        ) AS rn       
    FROM sped.reg_0000 r
    JOIN PARAMETROS p ON (p.cnpj_filtro IS NULL OR r.cnpj = p.cnpj_filtro)
    WHERE r.data_entrega <= p.dt_corte 
      AND r.dt_ini BETWEEN p.dt_ini_filtro AND p.dt_fim_filtro
),

ARQUIVOS_VALIDOS AS (
    SELECT 
        reg_0000_id, 
        dt_ini, 
        data_entrega, 
        cod_fin,
        cod_item_filtro,
        chave_acesso_filtro
    FROM ARQUIVOS_RANKING
    WHERE rn = 1
),

CTE_C100 AS (
    SELECT 
        c.id AS reg_c100_id,
        c.reg_0000_id,
        c.reg,
        c.cod_sit,
        c.ind_oper,
        c.ind_emit,
        c.chv_nfe,
        c.num_doc,
        c.cod_part,
        c.dt_doc,
        c.dt_e_s
    FROM sped.reg_c100 c
    INNER JOIN ARQUIVOS_VALIDOS a ON c.reg_0000_id = a.reg_0000_id
    WHERE (a.chave_acesso_filtro IS NULL OR c.chv_nfe = a.chave_acesso_filtro)
),

CTE_C170 AS (
    SELECT 
        i.reg_0000_id,
        i.reg_c100_id,
        i.reg,
        i.num_item,
        i.cod_item,
        i.cfop,
        i.cod_nat,
        i.cst_icms,
        i.aliq_icms,
        i.ind_mov,
        i.unid,
        i.qtd,
        i.vl_item,
        i.vl_desc,
        i.vl_bc_icms,
        i.vl_icms,
        i.vl_bc_icms_st,
        i.aliq_st,
        i.vl_icms_st,
        i.cst_ipi,
        i.cod_enq,
        i.vl_bc_ipi,
        i.aliq_ipi,
        i.vl_ipi,
        i.cod_cta,
        i.VL_ABAT_NT,
        REPLACE(REPLACE(REPLACE(LTRIM(i.cod_item, '0'), ' ',''), '.', ''),'-','') AS cod_item_limpo
    FROM sped.reg_c170 i
    INNER JOIN ARQUIVOS_VALIDOS a ON i.reg_0000_id = a.reg_0000_id
    WHERE (a.cod_item_filtro IS NULL 
           OR REPLACE(REPLACE(REPLACE(LTRIM(i.cod_item, '0'), ' ',''), '.', ''),'-','') = a.cod_item_filtro)
),

CTE_0200 AS (
    SELECT 
        p.reg_0000_id,
        p.cod_item,
        p.cod_barra,
        p.descr_item,
        p.tipo_item,
        p.cod_gen,
        p.cod_ncm,
        p.cest,
        p.unid_inv
    FROM sped.reg_0200 p
    INNER JOIN ARQUIVOS_VALIDOS a ON p.reg_0000_id = a.reg_0000_id
),

CTE_0400 AS (
    SELECT 
        n.reg_0000_id,
        n.cod_nat,
        n.descr_nat
    FROM SPED.REG_0400 n
    INNER JOIN ARQUIVOS_VALIDOS a ON n.reg_0000_id = a.reg_0000_id
)

SELECT
    TO_CHAR(arq.data_entrega, 'DD/MM/YYYY') AS dt_ultima_entrega,
    arq.cod_fin,
    CASE arq.cod_fin
        WHEN '0' THEN 'Remessa do arquivo original'
        WHEN '1' THEN 'Remessa do arquivo substituto'
        ELSE 'Outros'
    END AS descricao_fin,

    EXTRACT(YEAR FROM arq.dt_ini)            AS Ano_efd,
    TO_CHAR(arq.dt_ini, 'MM/YYYY')           AS periodo_efd,
    
    c100.reg                                 AS c100_reg,
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
        WHEN '08' THEN 'Documento Fiscal emitido com base em Regime Especial'
        ELSE 'Código desconhecido'
    END AS descricao_cod_sit,
    
    c100.ind_oper,
    /* CORREÇÃO: Transformado 0 e 1 em '0' e '1' */
    CASE c100.ind_oper
        WHEN '0' THEN 'ENTRADA'
        WHEN '1' THEN 'SAÍDA'
    END AS Oper,
    
    c100.IND_EMIT,
    /* CORREÇÃO: Transformado 0 e 1 em '0' e '1' */
    CASE c100.IND_EMIT
        WHEN '0' THEN 'Emissão própria'
        WHEN '1' THEN 'Terceiros'
    END AS Descricao_IND_EMIT,
    
    c100.chv_nfe,
    c100.num_doc,
    c100.cod_part,
    
    CASE 
        WHEN c100.dt_doc IS NOT NULL AND REGEXP_LIKE(c100.dt_doc, '^\d{8}$')
        THEN TO_DATE(c100.dt_doc, 'DDMMYYYY')
        ELSE NULL
    END AS dt_doc,
    CASE 
        WHEN c100.dt_e_s IS NOT NULL AND REGEXP_LIKE(c100.dt_e_s, '^\d{8}$')
        THEN TO_DATE(c100.dt_e_s, 'DDMMYYYY')
        ELSE NULL
    END AS dt_e_s,

    c170.reg                                 AS c170_reg,
    c170.num_item,
    c170.cod_item,
    c170.cod_item_limpo                      AS COD,

    r0200.cod_barra,
    r0200.descr_item,
    r0200.tipo_item,
    CASE r0200.tipo_item
        WHEN '00' THEN 'Mercadoria para Revenda'
        WHEN '01' THEN 'Matéria-prima'
        WHEN '02' THEN 'Embalagem'
        WHEN '03' THEN 'Produto em Processo'
        WHEN '04' THEN 'Produto Acabado'
        WHEN '05' THEN 'Subproduto'
        WHEN '06' THEN 'Produto Intermediário'
        WHEN '07' THEN 'Material de Uso e Consumo'
        WHEN '08' THEN 'Ativo Imobilizado'
        WHEN '09' THEN 'Serviços'
        WHEN '10' THEN 'Outros insumos'
        WHEN '99' THEN 'Outras'
        ELSE 'Tipo Desconhecido'
    END AS Descricao_tipo_item,
    
    r0200.cod_gen,
    CASE r0200.cod_gen
        WHEN '00' THEN 'Serviço'
        WHEN '01' THEN 'Animais vivos'
        ELSE 'Código Genérico ' || r0200.cod_gen
    END AS descricao_cod_gen,
    
    r0200.cod_ncm,
    r0200.cest,
    REGEXP_SUBSTR(r0200.cest, '^\d{2}') AS segmento_cest,
    cest_segmento.no_segmento,
    
    c170.cfop,
    cfop.DESCRICAO_CFOP,
    c170.cod_nat,
    cod_nat.descr_nat AS descricao_cod_nat,
    c170.cst_icms,
    cst.DESC_CST AS descricao_cst_icms,
    c170.aliq_icms,
    
    c170.ind_mov,
    /* CORREÇÃO: Transformado 0 e 1 em '0' e '1' */
    CASE c170.ind_mov
        WHEN '0' THEN 'Mov. Física SIM'
        WHEN '1' THEN 'Mov. Física NÃO'
    END AS Descricao_ind_mov,
    
    r0200.unid_inv,
    c170.unid,
    c170.qtd,
    c170.vl_item,
    c170.vl_desc,
    c170.vl_bc_icms,
    c170.vl_icms,
    c170.vl_bc_icms_st,
    c170.aliq_st,
    c170.vl_icms_st,
    c170.cst_ipi,
    c170.cod_enq,
    c170.vl_bc_ipi,
    c170.aliq_ipi,
    c170.vl_ipi,
    c170.cod_cta,
    c170.VL_ABAT_NT

FROM CTE_C170 c170
INNER JOIN ARQUIVOS_VALIDOS arq 
    ON arq.reg_0000_id = c170.reg_0000_id

INNER JOIN CTE_C100 c100 
    ON c100.reg_c100_id = c170.reg_c100_id

LEFT JOIN CTE_0200 r0200 
    ON r0200.reg_0000_id = c170.reg_0000_id 
    AND r0200.cod_item = c170.cod_item

LEFT JOIN CTE_0400 cod_nat 
    ON cod_nat.cod_nat = c170.cod_nat
    AND cod_nat.reg_0000_id = c170.reg_0000_id

LEFT JOIN BI.DM_CFOP cfop 
    ON cfop.CO_CFOP = c170.cfop

LEFT JOIN BI.DM_CST cst 
    ON cst.CO_CST = c170.cst_icms

LEFT JOIN BI.DM_CEST_SEGMENTO cest_segmento
    ON REGEXP_SUBSTR(r0200.cest, '^\d{2}') = cest_segmento.cod_segmento    

ORDER BY arq.dt_ini, c100.num_doc, c170.num_item;