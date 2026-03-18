WITH PARAMETROS AS (
    SELECT 
        :CNPJ AS cnpj_filtro,
        
        NVL(TO_DATE(:data_inicial, 'DD/MM/YYYY'), TO_DATE('01/01/1900', 'DD/MM/YYYY')) AS dt_ini_filtro,
        
        -- TRUNC remove a componente de horas
        NVL(TO_DATE(:data_final, 'DD/MM/YYYY'), TRUNC(SYSDATE)) AS dt_fim_filtro,
        
        NVL(TO_DATE(:data_limite_processamento, 'DD/MM/YYYY'), TRUNC(SYSDATE)) AS dt_corte
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
        CASE 
            WHEN reg_0000.cod_fin = 0 THEN '0 - Remessa do arquivo original'
            WHEN reg_0000.cod_fin = 1 THEN '1 - Remessa do arquivo substituto'
            ELSE 'Outros'    
        END AS desc_finalidade,
        
        -- Colunas de controle trazidas da CTE Parametros para filtro posterior
        p.dt_corte, 
        p.dt_ini_filtro, 
        p.dt_fim_filtro,
        
        -- Lógica de Versionamento:
        -- Particiona por Empresa e Mês de Referência (dt_inicio)
        -- Ordena por Data de Entrega decrescente (o mais recente fica com rn=1)
        ROW_NUMBER() OVER (
            PARTITION BY reg_0000.cnpj, reg_0000.dt_ini 
            -- CORREÇÃO: Adicionado critério de desempate pelo ID
            ORDER BY reg_0000.data_entrega DESC, reg_0000.id DESC
        ) AS rn       
    FROM sped.reg_0000 reg_0000
    JOIN PARAMETROS p ON reg_0000.cnpj = p.cnpj_filtro
    WHERE 
        reg_0000.data_entrega <= p.dt_corte -- Filtro de "Viagem no Tempo": Ignora retificações feitas após a data de corte
        AND reg_0000.dt_ini BETWEEN p.dt_ini_filtro AND p.dt_fim_filtro
)
SELECT
    TO_CHAR(arq.dt_ini, 'MM/YYYY') AS periodo_efd,
    
    -- CORREÇÃO: Sintaxe correta para trazer todas as colunas de uma tabela/alias
    reg_0190.*,
    
    arq.cod_fin_efd,
    arq.data_entrega AS data_entrega_efd_periodo
    
FROM sped.reg_0190 reg_0190
INNER JOIN ARQUIVOS_RANKING arq 
  ON reg_0190.reg_0000_id = arq.reg_0000_id
WHERE arq.rn = 1 /* Filtra apenas a versão mais recente (Original ou Retificadora Final) */
ORDER BY arq.dt_ini ASC