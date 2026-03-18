/*
 * CONSULTA: Extração de Base ICMS ST e FCP ST via XML
 * Objetivo: Conciliar valores de ST (Própria e Retida) e Fundo de Combate à Pobreza.
 * * --------------------------------------------------------------------------------
 * RESUMO DOS CAMPOS EXTRAÍDOS (Conforme Manual de Orientação do Contribuinte - MOC):
 * 1. Identificação: Chave de acesso, número do item e código do produto.
 * 2. ICMS ST (Gerado): Base e valor retido pelo Substituto (Indústria/Importador).
 * 3. ICMS ST (Retido): Valores de operações anteriores (CST 60 / CSOSN 500).
 * 4. FCP ST: Adicional para o Fundo de Combate à Pobreza na Substituição.
 * --------------------------------------------------------------------------------
 */

-- Define o ponto como separador decimal para leitura correta dos valores numéricos no XML
-- PRE: ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,';

WITH
    PARAMETROS AS (
        SELECT
:CNPJ AS cnpj_filtro
        FROM DUAL
    ),
    LISTA_NFES AS (
        -- Filtra NF-es autorizadas (CStat 100/150) onde o CNPJ é emitente ou destinatário
        SELECT DISTINCT
            f.CHAVE_ACESSO
        FROM bi.fato_nfe_detalhe f
            CROSS JOIN PARAMETROS p
        WHERE (
                f.co_destinatario = p.cnpj_filtro
                OR f.co_emitente = p.cnpj_filtro
            )
            AND f.INFPROT_CSTAT IN (100, 150)
    )

SELECT
    -- 1. CAMPOS DE IDENTIFICAÇÃO (Localização da nota e do item)
    x.chave_acesso, -- <chNFe>: Chave de 44 dígitos (ID único da NF-e)
    xml_item.prod_nitem, -- @nItem: Número sequencial do item na nota
    xml_item.prod_cprod, -- <cProd>: Código interno do produto do emitente

-- 2. ICMS SUBSTITUIÇÃO TRIBUTÁRIA (Operação Própria/Gerada pelo Substituto)
xml_item.icms_vBCST, -- <vBCST>: Base de cálculo do ICMS ST (inclui MVA/Pauta)
xml_item.icms_vICMSST, -- <vICMSST>: Valor do imposto ST retido na operação

-- 3. ICMS ST RETIDO ANTERIORMENTE (CST 60 / CSOSN 500 - Distribuidores/Varejistas)
xml_item.icms_vICMSSubstituto, -- <vICMSSubstituto>: ICMS próprio do substituto (base para ressarcimento)
xml_item.icms_vICMSSTRet, -- <vICMSSTRet>: Valor do ST que já foi pago em etapas anteriores

-- 4. FUNDO DE COMBATE À POBREZA (FCP ST)
xml_item.icms_vBCFCPST, -- <vBCFCPST>: Base de cálculo do adicional de FCP ST
    xml_item.icms_pFCPST,   -- <pFCPST>: Percentual/Alíquota do FCP ST (ex: 2.00)
    xml_item.icms_vFCPST    -- <vFCPST>: Valor monetário do FCP ST
FROM bi.nfe_xml x
INNER JOIN LISTA_NFES l ON x.chave_acesso = l.chave_acesso
CROSS JOIN XMLTABLE(
    -- Define o Namespace padrão da NF-e para correta navegação no DOM
    XMLNAMESPACES (DEFAULT 'http://www.portalfiscal.inf.br/nfe'),
    -- '//det': Explode o XML para criar uma linha por item, independente da hierarquia
    '//det' PASSING x.xml 
    COLUMNS
        Prod_nItem           NUMBER       PATH '@nItem',
        PROD_cProd           VARCHAR2(74) PATH 'prod/cProd',

/* * OBSERVAÇÕES TÉCNICAS:
* - O uso de '//' nos paths (ex: 'imposto/ICMS//vBCST') permite buscar a tag 
* dentro de qualquer grupo de ICMS (ICMS10, ICMS60, ICMS70, etc).
* - 'DEFAULT 0' evita retornos nulos (NULL) quando a tag não existe no XML.
*/

-- Extração de ICMS ST Gerado
icms_vBCST NUMBER PATH 'imposto/ICMS//vBCST' DEFAULT 0,
icms_vICMSST NUMBER PATH 'imposto/ICMS//vICMSST' DEFAULT 0,

-- Extração de ICMS ST Retido Anteriormente
icms_vICMSSubstituto NUMBER PATH 'imposto/ICMS//vICMSSubstituto' DEFAULT 0,
icms_vICMSSTRet NUMBER PATH 'imposto/ICMS//vICMSSTRet' DEFAULT 0,

-- Extração de FCP ST
icms_vBCFCPST        NUMBER       PATH 'imposto/ICMS//vBCFCPST'        DEFAULT 0,
        icms_pFCPST          NUMBER       PATH 'imposto/ICMS//pFCPST'          DEFAULT 0,
        icms_vFCPST          NUMBER       PATH 'imposto/ICMS//vFCPST'          DEFAULT 0
) xml_item
ORDER BY x.chave_acesso, xml_item.prod_nitem;