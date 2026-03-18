WITH parametros AS (
    SELECT 
        :CNPJ AS cnpj
         
        
    FROM dual
),
xml_saida AS (
    -- 1. Captura de Notas Fiscais Emitidas (Operações de Saída/Débito)
    SELECT
        nfe.chave_acesso,
        nfe.ide_serie AS serie,
        nfe.nnf AS num_doc,
        nfe.tot_vnf AS valor_total_nfe,
        nfe.tot_vicms AS icms_destacado_nfe,
        nfe.dhemi AS data_emissao,
        'NF-e (Mod 55)' AS modelo
    FROM bi.fato_nfe_detalhe nfe
    CROSS JOIN parametros p
    WHERE nfe.co_emitente = p.cnpj
      AND nfe.co_tp_nf = 1 -- Operação de Saída
      AND nfe.infprot_cstat IN ('100', '150') -- Apenas documentos com uso autorizado
      AND nfe.seq_nitem = '1'
      
      

    UNION ALL

    -- 2. Captura de NFC-e (Varejo/Saída)
    SELECT
        nfce.chave_acesso,
        nfce.ide_serie AS serie,
        nfce.nnf AS num_doc,
        nfce.tot_vnf AS valor_total_nfe,
        nfce.tot_vicms AS icms_destacado_nfe,
        nfce.dhemi AS data_emissao,
        'NFC-e (Mod 65)' AS modelo
    FROM bi.fato_nfce_detalhe nfce
    CROSS JOIN parametros p
    WHERE nfce.co_emitente = p.cnpj
      AND nfce.infprot_cstat IN ('100', '150')
      AND nfce.seq_nitem = '1'
      
      
),
efd_saida AS (
    -- 3. Captura dos valores escriturados na EFD (Bloco C) para Operações de Saída
    SELECT 
        c100.chv_nfe,
        c100.vl_icms AS icms_escriturado_efd
    FROM sped.reg_c100 c100
    INNER JOIN sped.reg_0000 r0000 ON c100.reg_0000_id = r0000.id
    INNER JOIN bi.dm_efd_arquivo_valido arqv ON c100.reg_0000_id = arqv.reg_0000_id
    CROSS JOIN parametros p
    WHERE r0000.cnpj = p.cnpj
      AND c100.ind_oper = '1' -- Garante que estamos olhando apenas para saídas na EFD
      
),
debitos_a_menor AS (
    -- 4. Cruzamento e Identificação do Débito Declarado a Menor
    SELECT
        x.modelo,
        x.chave_acesso,
        x.serie,
        x.num_doc,
        x.data_emissao,
        x.valor_total_nfe,
        x.icms_destacado_nfe,
        NVL(e.icms_escriturado_efd, 0) AS icms_escriturado_efd,
        (x.icms_destacado_nfe - NVL(e.icms_escriturado_efd, 0)) AS diferenca_icms_nao_debitado
    FROM xml_saida x
    INNER JOIN efd_saida e ON x.chave_acesso = e.chv_nfe
    WHERE NVL(e.icms_escriturado_efd, 0) < x.icms_destacado_nfe
),
pendencias_fisconforme AS (
    -- 5. Busca relacionamento da chave com a malha
    SELECT f.chave_acesso, f.malhas_id, f.referencia_malhas_id
    FROM app_pendencia.vw_fisconforme_chave_nota f
    CROSS JOIN parametros p
    WHERE f.cpf_cnpj = p.cnpj
),
dados_pendencias AS (
    -- 6. Busca os detalhes da malha na tabela de pendências
    SELECT pen.id AS id_pendencia, pen.malhas_id, pen.referencia_malhas_id, pen.periodo, pen.status, pen.data_ciencia
    FROM app_pendencia.pendencias pen
    CROSS JOIN parametros p
    WHERE pen.cpf_cnpj = p.cnpj
),
dados_malha AS (
    -- 7. Busca o título da malha
    SELECT m.id, m.titulo FROM app_pendencia.malhas m 
),
dados_notificacao AS (
    -- 8. Busca detalhes da notificação
    SELECT notif.id_fisconforme, notif.id_notificacao, notif.tp_status, notif.dt_envio, notif.dt_ciencia, notif.co_cpf_cnpj_ciencia, notif.no_pessoa_ciencia
    FROM bi.fato_det_notificacao notif
    CROSS JOIN parametros p
    WHERE notif.co_cnpj_notif = p.cnpj
)

/* Passo Final: Combinação dos Campos da Imagem com os Valores do Débito */
SELECT 
    -- ===== CAMPOS DA IMAGEM =====
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
    
    -- ===== CAMPOS DA AUDITORIA (NOVA LÓGICA) =====
    d.modelo,
    d.chave_acesso,
    d.serie,
    d.num_doc,
    d.data_emissao,
    d.valor_total_nfe,
    d.icms_destacado_nfe,
    d.icms_escriturado_efd,
    d.diferenca_icms_nao_debitado,
    'Débito a Menor na EFD' AS status_auditoria

FROM debitos_a_menor d
LEFT JOIN pendencias_fisconforme f ON d.chave_acesso = f.chave_acesso
LEFT JOIN dados_pendencias dp ON f.malhas_id = dp.malhas_id AND f.referencia_malhas_id = dp.referencia_malhas_id 
LEFT JOIN dados_malha m ON f.malhas_id = m.id    
LEFT JOIN dados_notificacao dn ON dp.id_pendencia = dn.id_fisconforme
ORDER BY 
    d.data_emissao DESC, 
    d.num_doc DESC;