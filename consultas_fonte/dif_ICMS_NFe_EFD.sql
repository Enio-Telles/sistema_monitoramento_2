    -- Débito a Menor na EFD (Modelos 55 e 65)
    WITH xml_saida AS (
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
        WHERE nfe.co_emitente = :CNPJ
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
        WHERE nfce.co_emitente = :CNPJ
          AND nfce.infprot_cstat IN ('100', '150')
          AND nfce.seq_nitem = '1'
          
          
    ),
    
    efd_saida AS (
        -- 3. Captura dos valores escriturados na EFD (Bloco C) para Operações de Saída
        SELECT 
            c100.chv_nfe,
            c100.vl_icms AS icms_escriturado_efd
        FROM sped.reg_c100 c100
        INNER JOIN sped.reg_0000 r0000 
            ON c100.reg_0000_id = r0000.id
        INNER JOIN bi.dm_efd_arquivo_valido arqv 
            ON c100.reg_0000_id = arqv.reg_0000_id
        WHERE r0000.cnpj = :CNPJ
          AND c100.ind_oper = '1' -- Garante que estamos olhando apenas para saídas na EFD
          -- Restringir a data de entrega/apuração ajuda a usar partições da EFD no Oracle
          
    )
    
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
        (x.icms_destacado_nfe - NVL(e.icms_escriturado_efd, 0)) AS diferenca_icms_nao_debitado,
        'Débito a Menor na EFD' AS status_auditoria
    FROM xml_saida x
    INNER JOIN efd_saida e 
        ON x.chave_acesso = e.chv_nfe
    -- O filtro vital que atende à sua regra de malha:
    WHERE NVL(e.icms_escriturado_efd, 0) < x.icms_destacado_nfe
    ORDER BY 
        x.data_emissao, 
        x.num_doc;