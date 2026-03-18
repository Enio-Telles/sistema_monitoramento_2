WITH
    PARAMETROS AS (
        SELECT
            :CNPJ AS cnpj_filtro,
            -- Data de Corte (Viagem no Tempo): Define qual era a versão "ativa" do arquivo naquela data
            NVL (
                TO_DATE(:data_limite_processamento, 'DD/MM/YYYY'),
                TRUNC(SYSDATE)
            ) AS dt_corte
        FROM dual
    ),

ARQUIVOS_RANKING AS (
    SELECT
        reg_0000.id as reg_0000_id,
        reg_0000.cnpj,
        reg_0000.cod_fin AS cod_fin_efd,
        reg_0000.dt_ini,
        reg_0000.dt_fin,
        reg_0000.data_entrega,
        p.dt_corte, 
        
        -- Ranking: Garante apenas a última versão válida do arquivo para o período
        ROW_NUMBER() OVER (
            PARTITION BY reg_0000.cnpj, reg_0000.dt_ini 
            ORDER BY reg_0000.data_entrega DESC
        ) AS rn       
    FROM sped.reg_0000 reg_0000
    JOIN PARAMETROS p ON reg_0000.cnpj = p.cnpj_filtro
    WHERE 
        reg_0000.data_entrega <= p.dt_corte
)

SELECT
    TO_CHAR(arq.dt_ini, 'YYYY/MM') AS periodo_efd,
    e111.cod_aj_apur AS codigo_ajuste,
    aj.no_cod_aj AS descricao_codigo_ajuste,
    e111.descr_compl_aj AS descr_compl,  
    e111.vl_aj_apur AS valor_ajuste,
    arq.data_entrega AS data_entrega_efd_periodo,
    arq.cod_fin_efd

FROM ARQUIVOS_RANKING arq
    INNER JOIN sped.reg_e111 e111 ON e111.reg_0000_id = arq.reg_0000_id
    -- Join reintegrado conforme solicitação
    LEFT JOIN bi.dm_efd_ajustes aj ON e111.cod_aj_apur = RTRIM(aj.co_cod_aj)

WHERE 
    arq.rn = 1 -- Apenas versão ativa do arquivo


ORDER BY 
    periodo_efd, 
    e111.cod_aj_apur