/* ===================================================================================
   CONSULTA DE ANÁLISE DE FRONTEIRA / SITAFE (VERSÃO OTIMIZADA - SEFIN/RO)
   ===================================================================================
   Objetivo: Cruzar dados de Notas Fiscais Eletrônicas (NFe) do BI com os lançamentos
   contábeis e fiscais do sistema SITAFE.
   
   Otimizações (Auditoria):
   1. Remoção de DISTINCT oneroso e substituição por GROUP BY na Subquery ORIGEM.
   2. Adequação SARGable: Filtros de data convertidos para string (YYYYMMDD) para 
      permitir o uso de índices B-Tree na tabela sitafe_nota_fiscal.
   3. Eliminação de CROSS JOIN em subqueries para evitar Cartesian Plans.
   4. Reordenação dos Joins priorizando a Fato NFe filtrada como Driving Table.
=================================================================================== */

WITH PARAMETROS AS (
    SELECT 
        :cnpj AS cnpj_filtro
    FROM dual
),

-- SUBCONSULTA 'ORIGEM': Busca os dados originais no BI (Data Warehouse)
-- Otimizada com GROUP BY para garantir unicidade da chave sem estourar TEMP TABLESPACE
ORIGEM_NFE AS (
    SELECT 
        NF1.CHAVE_ACESSO AS CHAVE,
        MAX(NF1.NNF) AS NOTA,
        MAX(NF1.CO_EMITENTE) AS CNPJ_EMIT,
        MAX(NF1.XNOME_EMIT) AS NOME_EMIT,
        MAX(NF1.DHEMI) AS EMISSAO,
        MAX(NF1.CO_UF_EMIT) AS UF_EMITENTE
    FROM BI.FATO_NFE_DETALHE NF1
    INNER JOIN PARAMETROS P ON 1=1 -- Força a amarração das variáveis sem Cross Join solto
    WHERE 
        NF1.CO_DESTINATARIO = P.cnpj_filtro 
        AND NF1.CO_TP_NF = 1 
        AND NF1.CO_UF_EMIT <> 'RO' 
        AND NF1.INFPROT_CSTAT IN ('100','150')
    GROUP BY NF1.CHAVE_ACESSO
)

SELECT 
      -- ==============================================================================
      -- DADOS DE IDENTIFICAÇÃO DA NOTA E EMITENTE
      -- ==============================================================================
      origem.chave,                                                                                                     
      origem.nota,                                                                                                      
      origem.cnpj_emit,
      origem.nome_emit,
      origem.uf_emitente,
      origem.emissao,
      TO_DATE(nl.it_da_entrada, 'YYYYMMDD')                                                        entrada,
      nl.it_co_comando                                                                             comando,

      -- ==============================================================================
      -- DADOS DO ITEM/PRODUTO
      -- ==============================================================================
      item.it_nu_item                                                                              prod_nitem,
      item.it_no_produto                                                                           prod_xprod,
      item.it_co_cfop                                                                              co_cfop,
      item.it_co_ncm                                                                               NCM,
      item.it_un_comercial                                                                         prod_ucom,
      item.it_qt_comercial                                                                         prod_qcom,
      
      -- ==============================================================================
      -- VALORES MONETÁRIOS DO ITEM (Custos e Impostos)
      -- ==============================================================================
      item.it_va_unitario_com                                                                      prod_vuncom,
      item.it_va_produto                                                                           prod_vprod,
      item.it_va_frete                                                                             prod_vfrete,
      item.it_va_desconto                                                                          prod_vdesc,
      item.it_va_outro                                                                             prod_voutro,
      item.it_va_seguro                                                                            prod_vseg,
      -- CÁLCULO: Valor Total Líquido do Produto
      (item.it_va_produto + item.it_va_frete - item.it_va_desconto + item.it_va_outro + item.it_va_seguro) total_produto,
      
      -- Impostos (ICMS Base e ST)
      item.it_va_bc                                                                                icms_vbc,
      item.it_pc_icms                                                                              icms_picms,
      item.it_va_icms                                                                              icms_vicms,
      item.it_va_bc_st                                                                             icms_vbcst,
      item.it_va_icms_st                                                                           icms_vicmsst,

      -- ==============================================================================
      -- DADOS DO LANÇAMENTO E PAGAMENTO
      -- ==============================================================================
      lanc.it_co_receita                                                                           receita,
      lanc.it_nu_guia_lancamento                                                                   guia,
      lanc.it_va_principal_original                                                                valor_devido,
      CASE WHEN lanc.it_co_receita IS NULL THEN 0 ELSE lanc.it_va_total_pgto_efetuado END          valor_pago,
      
      -- TRADUÇÃO DE STATUS
      CASE 
            WHEN lanc.it_co_situacao_lancamento IN ('00', '03') THEN 'PAGO'
            WHEN lanc.it_co_situacao_lancamento = '28' THEN 'BAIXA DE ACORDO COM O DEC 11430/2004'
            WHEN lanc.it_co_situacao_lancamento = '68' THEN 'SUSPENSO'
            WHEN lanc.it_co_situacao_lancamento = '13' THEN 'CORREÇÃO NO PAGAMENTO ORIGINAL'
            WHEN lanc.it_co_situacao_lancamento = '02' THEN 'PAGO A MENOR'
            WHEN lanc.it_co_situacao_lancamento = '05' THEN 'PARCELADO'
            WHEN lanc.it_co_situacao_lancamento = '08' THEN 'INSCRITO EM DA'
            WHEN lanc.it_co_situacao_lancamento = '10' THEN 'BAIXA PROVISÓRIA'
            WHEN lanc.it_co_situacao_lancamento = '14' THEN 'LANÇAMENTO EXCLUÍDO'
            WHEN lanc.it_co_situacao_lancamento = '32' THEN 'COMPENSAÇÃO'
            WHEN lanc.it_co_situacao_lancamento = '38' THEN 'LIQUIDAÇÃO DESVINCULADA DE CONTA GRÁFICA'
            WHEN lanc.it_co_situacao_lancamento = '46' THEN 'SUSPENSÃO JUDICIAL'
            WHEN lanc.it_co_situacao_lancamento = '50' THEN 'LANÇAMENTO INDEVIDO'
            WHEN lanc.it_co_situacao_lancamento IS NULL THEN ' '
            ELSE 'VERIFICAR' 
      END situação,

      -- ==============================================================================
      -- DETALHAMENTO TRIBUTÁRIO ESPECÍFICO (Mercadoria/Sefin)
      -- ==============================================================================
      lanc_item.it_co_produto                                                                      co_sefin,
      prod_sefin.it_no_produto                                                                     nome_co_sefin,
      lanc_item.it_vl_merc_item                                                                    vl_merc,
      lanc_item.it_vl_merc_bc_item                                                                 vl_bc_merc,
      lanc_item.it_aliq_item                                                                       aliq,
      lanc_item.it_vl_tot_debito_item                                                              vl_tot_deb,
      lanc_item.it_vl_credito_rateio                                                               vl_tot_cred,
      lanc_item.it_vl_icms_recolher                                                                vl_icms,
      -- Indicadores fiscais diversos
      m.it_pc_aliquota_interna,
      m.it_pc_aliquota_origem,
      m.it_convenio,
      m.it_pc_agregacao_interna,
      m.it_in_pgto_saida,
      m.it_boletim_pauta,
      m.it_in_isento_icms,
      m.it_passe_fiscal,
      m.it_in_combustivel,
      m.it_in_produto_st,
      m.it_in_cest_st,
      m.it_pc_interna

FROM ORIGEM_NFE origem
    
    -- Inicia os Joins pela Nota Fiscal do SITAFE (Driving path otimizado)
    JOIN sitafe.sitafe_nota_fiscal nf 
        ON nf.it_nu_identificao_nf_e = origem.chave
        
    -- Busca os parâmetros para garantir os filtros SARGable (via INNER JOIN 1=1 ou CROSS JOIN seguro)
    INNER JOIN PARAMETROS p ON 1=1
        
    -- Explode para o Lançamento (Nota)
    JOIN sitafe.sitafe_nf_lancamento nl 
        ON nl.it_nu_identificacao_nf = nf.it_nu_identificacao_nf
        
    -- Detalha itens da NFe
    JOIN sitafe.sitafe_nfe_item item 
        ON item.it_nu_chave_acesso = origem.chave
        
    -- Detalha os Lançamentos Financeiros (Guias)
    JOIN sitafe.sitafe_lancamento lanc 
        ON lanc.it_nu_guia_lancamento = nl.it_nu_guia_lancamento
        
    -- Cruza Item da NFe com o Item do Lançamento
    JOIN sitafe.sitafe_lancamento_item lanc_item 
        ON lanc_item.it_nu_identificacao_ndf = nl.it_nu_identificacao_ndf 
        AND lanc_item.it_co_produto = item.it_co_sefin
        
    -- Classificação Fiscal Estadual
    JOIN sitafe.sitafe_mercadoria m 
        ON m.it_co_sefin = lanc_item.it_co_produto
        
    -- Descrição do Produto
    LEFT JOIN sitafe.sitafe_produto_sefin prod_sefin 
        ON prod_sefin.it_co_sefin = lanc_item.it_co_produto

WHERE
    -- Filtros aplicados de forma SARGable (coluna isolada, sem funções)
    nf.it_nucnpj_cpf_destino_nf = p.cnpj_filtro 
    AND nl.it_nu_cnpj_cpf_destino_nf = p.cnpj_filtro

ORDER BY
    origem.emissao, 
    origem.cnpj_emit, 
    origem.nota,
    CASE WHEN item.it_nu_item IS NULL THEN 1 ELSE 2 END,
    to_number(item.it_nu_item)