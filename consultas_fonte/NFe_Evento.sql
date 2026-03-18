/*
 * EXTRAÇÃO SIMPLIFICADA: NFe Eventos (para cruzamento NFe vs EFD C100)
 * Traz os eventos vinculados às NFe do contribuinte.
 * Colunas usadas por: cruzamento_nfe_nao_reg_efd.py
 */
WITH parametros AS (
    SELECT :CNPJ AS cnpj_filtro
    FROM DUAL
)
SELECT DISTINCT
    ev.chave_acesso,
    ev.nsu AS nsu_evento,
    ev.evento_dhevento,
    ev.evento_tpevento,
    ev.evento_descevento
FROM bi.dm_eventos ev
JOIN bi.fato_nfe_detalhe nfe ON nfe.chave_acesso = ev.chave_acesso AND nfe.seq_nitem = 1
CROSS JOIN parametros p
WHERE
    (nfe.co_destinatario = p.cnpj_filtro OR nfe.co_emitente = p.cnpj_filtro)
ORDER BY ev.chave_acesso, ev.evento_dhevento, ev.nsu
