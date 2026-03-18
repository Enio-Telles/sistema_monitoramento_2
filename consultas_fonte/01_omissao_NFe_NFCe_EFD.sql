WITH docs_saida AS (
    SELECT 
        d.chave_acesso,
        d.ide_serie,
        d.nnf,
        d.tot_vnf,
        d.tot_vicms,
        d.dhemi,
        d.co_uf_emit,
        d.co_uf_dest,
        d.co_emitente,
        d.co_destinatario,
        d.infprot_cstat,
        SUBSTR(d.chave_acesso, 21, 2) AS co_modelo
    FROM bi.fato_nfe_detalhe d
    WHERE d.co_emitente = :CNPJ
      AND d.co_tp_nf = 1
      AND SUBSTR(d.chave_acesso, 21, 2) IN ('55', '65')
      AND d.infprot_cstat IN ('100', '150')
      AND d.seq_nitem = 1
),
efd_saidas AS (
    SELECT DISTINCT c100.chv_nfe AS chave_efd
    FROM sped.reg_c100 c100
    JOIN sped.reg_0000 r0000
      ON r0000.id = c100.reg_0000_id
    JOIN bi.dm_efd_arquivo_valido arqv
      ON arqv.reg_0000_id = c100.reg_0000_id
    WHERE r0000.cnpj = :CNPJ
      AND c100.chv_nfe IS NOT NULL
      -- se existir no seu layout:
      -- AND c100.ind_oper = '1'
      -- AND c100.cod_mod IN ('55', '65')
),
notas_omissas AS (
    SELECT d.*
    FROM docs_saida d
    WHERE NOT EXISTS (
        SELECT 1
        FROM efd_saidas e
        WHERE e.chave_efd = d.chave_acesso
    )
),
evento_ordenado AS (
    SELECT
        ev.chave_acesso,
        ev.evento_descevento
            || ' (' || TO_CHAR(ev.evento_dhevento, 'DD/MM/YY HH24:MI') || ')' AS desc_evento,
        ROW_NUMBER() OVER (
            PARTITION BY ev.chave_acesso
            ORDER BY ev.nsu DESC, ev.evento_dhevento DESC
        ) AS rn
    FROM bi.dm_eventos ev
    WHERE EXISTS (
        SELECT 1
        FROM notas_omissas o
        WHERE o.chave_acesso = ev.chave_acesso
    )
),
max_evento AS (
    SELECT
        chave_acesso,
        desc_evento
    FROM evento_ordenado
    WHERE rn = 1
),
pendencias_fisconforme AS (
    SELECT
        f.chave_acesso,
        f.malhas_id,
        f.referencia_malhas_id
    FROM app_pendencia.vw_fisconforme_chave_nota f
    WHERE f.cpf_cnpj = :CNPJ
),
pendencias_ordenadas AS (
    SELECT
        pen.id AS id_pendencia,
        pen.malhas_id,
        pen.referencia_malhas_id,
        pen.periodo,
        pen.status,
        pen.data_ciencia,
        ROW_NUMBER() OVER (
            PARTITION BY pen.malhas_id, pen.referencia_malhas_id
            ORDER BY pen.id DESC
        ) AS rn
    FROM app_pendencia.pendencias pen
    WHERE pen.cpf_cnpj = :CNPJ
),
dados_pendencias AS (
    SELECT
        id_pendencia,
        malhas_id,
        referencia_malhas_id,
        periodo,
        status,
        data_ciencia
    FROM pendencias_ordenadas
    WHERE rn = 1
),
dados_malha AS (
    SELECT
        m.id,
        m.titulo
    FROM app_pendencia.malhas m
),
notificacao_ordenada AS (
    SELECT
        notif.id_fisconforme,
        notif.id_notificacao,
        notif.tp_status,
        notif.dt_envio,
        notif.dt_ciencia,
        notif.co_cpf_cnpj_ciencia,
        notif.no_pessoa_ciencia,
        ROW_NUMBER() OVER (
            PARTITION BY notif.id_fisconforme
            ORDER BY NVL(notif.dt_envio, notif.dt_ciencia) DESC, notif.id_notificacao DESC
        ) AS rn
    FROM bi.fato_det_notificacao notif
    WHERE notif.co_cnpj_notif = :CNPJ
),
dados_notificacao AS (
    SELECT
        id_fisconforme,
        id_notificacao,
        tp_status,
        dt_envio,
        dt_ciencia,
        co_cpf_cnpj_ciencia,
        no_pessoa_ciencia
    FROM notificacao_ordenada
    WHERE rn = 1
)
SELECT
    dp.id_pendencia,
    dn.id_notificacao,
    f.malhas_id,
    m.titulo AS titulo_malha,
    dp.periodo,
    CASE dp.status
        WHEN 0 THEN '0 - pendente'
        WHEN 1 THEN '1 - contestado'
        WHEN 2 THEN '2 - resolvido'
        WHEN 3 THEN '3 - acao fiscal'
        WHEN 4 THEN '4 - pendente indeferido'
        WHEN 5 THEN '5 - deferido'
        WHEN 6 THEN '6 - notificado'
        WHEN 7 THEN '7 - deferido automaticamente'
        WHEN 8 THEN '8 - aguardando autorizacao'
        WHEN 9 THEN '9 - cancelado'
        WHEN 11 THEN '11 - inapta - 5 anos'
        WHEN 12 THEN '12 - pre-fiscalizacao'
        ELSE TO_CHAR(dp.status)
    END AS status_pendencia,
    dn.tp_status AS status_notificacao,
    dn.dt_envio AS data_envio_notificacao,
    NVL(dn.dt_ciencia, dp.data_ciencia) AS data_ciencia_consolidada,
    dn.co_cpf_cnpj_ciencia AS cnpj_cpf_assinante,
    dn.no_pessoa_ciencia AS nome_assinante,
    CASE
        WHEN n.co_modelo = '55' THEN 'Saída Omissa - NF-e'
        WHEN n.co_modelo = '65' THEN 'Saída Omissa - NFC-e'
        ELSE 'Saída Omissa'
    END AS operacao,
    n.chave_acesso,
    n.ide_serie AS serie,
    n.nnf,
    n.tot_vnf AS valor_nota,
    n.tot_vicms AS valor_icms,
    n.dhemi AS data_emissao,
    n.infprot_cstat AS status_nfe,
    NVL(mev.desc_evento, 'SEM EVENTO') AS ultimo_evento
FROM notas_omissas n
LEFT JOIN max_evento mev
       ON mev.chave_acesso = n.chave_acesso
LEFT JOIN pendencias_fisconforme f
       ON f.chave_acesso = n.chave_acesso
LEFT JOIN dados_pendencias dp
       ON dp.malhas_id = f.malhas_id
      AND dp.referencia_malhas_id = f.referencia_malhas_id
LEFT JOIN dados_malha m
       ON m.id = f.malhas_id
LEFT JOIN dados_notificacao dn
       ON dn.id_fisconforme = dp.id_pendencia
ORDER BY n.dhemi DESC;