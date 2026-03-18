
WITH PARAMETROS AS (
    
    SELECT 
        :CNPJ AS cnpj_filtro,
        
        NVL(TO_DATE(:data_limite_processamento, 'DD/MM/YYYY'), TRUNC(SYSDATE)) AS dt_corte
    FROM dual
),
ARQUIVOS_PROCESSADOS AS (
    SELECT
        r.id as reg_0000_id,
        r.reg,
        r.cod_ver,
        r.cod_fin,
        r.dt_ini,
        r.dt_fin,
        r.nome,
        r.cnpj,
        r.cpf,
        r.uf,
        r.ie,
        r.cod_mun,
        r.im,
        r.suframa,
        r.ind_perfil,
        r.ind_ativ,
        r.arquivo_nome,
        r.data_entrega,
        r.created_at,
        r.updated_at,
        r.reg_1,
        r.reg_c,
        r.reg_d,
        r.reg_e,
        r.reg_g,
        r.reg_h,
        r.reg_k,
        r.arquivo_tamanho,
        p.dt_corte,

-- COLUNA 1: ORDEM
-- 1 = Último (Mais recente/Ativo)
-- 2 = Penúltimo (Substituído)
-- 3 = Antepenúltimo...
ROW_NUMBER() OVER (
    PARTITION BY
        r.cnpj,
        r.dt_ini
        -- Adicionado r.id DESC como critério de desempate (tie-breaker) para evitar aleatoriedade
    ORDER BY r.data_entrega DESC, r.id DESC
) AS ordem,

-- COLUNA 2: QUANTIDADE DE ENVIOS
-- Conta quantos ficheiros existem para este mês/cnpj até à data de corte

COUNT(*) OVER (
    PARTITION BY
        r.cnpj,
        r.dt_ini
) AS qtd_envios
FROM sped.reg_0000 r
    JOIN PARAMETROS p ON r.cnpj = p.cnpj_filtro
WHERE
    -- Filtro de "Viagem no Tempo" (Garante a visão dos dados na data de corte)
    r.data_entrega <= p.dt_corte

)
SELECT 
    reg_0000_id,
    reg,
    cod_ver,
    cod_fin,
    CASE 
       WHEN cod_fin = 0 THEN '0 - Remessa do arquivo original'
       WHEN cod_fin = 1 THEN '1 - Remessa do arquivo substituto'
       ELSE 'Outros'    
    END AS finalidade_arquivo,
    --dt_ini,
    dt_fin,
    nome,
    cnpj,
    cpf,
    uf,
    ie, -- inscricao estadual
    cod_mun,
    im, -- inscricao municipal
    suframa,
    --ind_perfil,
    CASE 
       WHEN ind_perfil = 'A' THEN 'Perfil A'
       WHEN ind_perfil = 'B' THEN 'Perfil B'
       WHEN ind_perfil = 'C' THEN 'Perfil C'
       ELSE 'Outros'    
    END AS ind_perfil,
    --ind_ativ,
     CASE 
       WHEN ind_ativ = 0 THEN '0 – Industrial ou equiparado a industrial'
       WHEN ind_ativ = 1 THEN '1 – Outros.'
       ELSE 'Outros'    
    END AS ind_ativ,   
    arquivo_nome,
    created_at,
    updated_at,
    reg_1,
    reg_c,
    reg_d,
    reg_e,
    reg_g,
    reg_h,
    reg_k,
    arquivo_tamanho,
    TO_CHAR(dt_ini, 'MM/YYYY') AS periodo_efd,
    data_entrega,

-- Tradução do Código de Finalidade

ordem, -- Será sempre 1 nesta visualização
qtd_envios, -- Indica se houve retificações (se > 1)
dt_corte AS data_limite_processamento
FROM ARQUIVOS_PROCESSADOS
WHERE
    ordem = 1 -- Filtra apenas o ficheiro ativo na data de corte
ORDER BY dt_ini;