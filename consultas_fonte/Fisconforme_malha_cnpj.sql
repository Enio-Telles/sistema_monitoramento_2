WITH PendenciasRankeadas AS (
    SELECT 
        dp.id AS id_pendencia, 
        dn.id_notificacao,
        dp.malhas_id,
        m.titulo AS titulo_malha,                    
        dp.periodo,                                  
        CASE dp.status
            WHEN 0  THEN '0 - pendente'
            WHEN 1  THEN '1 - contestado'
            WHEN 2  THEN '2 - resolvido'
            WHEN 3  THEN '3 - acao fiscal'
            WHEN 4  THEN '4 - pendente indeferido'
            WHEN 5  THEN '5 - deferido'
            WHEN 6  THEN '6 - notificado'
            WHEN 7  THEN '7 - deferido automaticamente'
            WHEN 8  THEN '8 - aguardando autorizacao'
            WHEN 9  THEN '9 - cancelado'
            WHEN 11 THEN '11 - inapta - 5 anos'
            WHEN 12 THEN '12 - pre-fiscalizacao'
            ELSE TO_CHAR(dp.status)
        END AS status_pendencia,
        dn.tp_status AS status_notificacao,
        -- Descomentamos a data de ciência para usá-la na ordenação
        NVL(dn.dt_ciencia, dp.data_ciencia) AS data_ciencia_consolidada,
        
        -- Função analítica que cria um ranking ordenado pela data mais recente
        ROW_NUMBER() OVER (
            PARTITION BY dp.id 
            ORDER BY NVL(dn.dt_ciencia, dp.data_ciencia) DESC NULLS LAST
        ) AS rn

    FROM app_pendencia.pendencias dp
    LEFT JOIN app_pendencia.malhas m 
        ON dp.malhas_id = m.id
    LEFT JOIN bi.fato_det_notificacao dn 
        ON dp.id = dn.id_fisconforme
    WHERE dp.cpf_cnpj = :CNPJ
      --AND dp.malhas_id IN (10061, 10120)
      --AND dp.status IN (0, 4)
      --AND dp.periodo < '202601'
)
-- Consulta final filtrando apenas a linha mais recente (Ranking = 1)
SELECT 
    id_pendencia,
    id_notificacao,
    malhas_id,
    titulo_malha,
    periodo,
    status_pendencia
    status_notificacao,
    data_ciencia_consolidada
FROM PendenciasRankeadas
WHERE rn = 1;