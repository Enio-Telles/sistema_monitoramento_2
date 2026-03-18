WITH PARAMETROS AS (
    -- 1. Define os filtros da consulta
    SELECT 
        :CNPJ AS cnpj_filtro,
        NVL(TO_DATE(:data_inicial, 'DD/MM/YYYY'), TO_DATE('01/01/1900', 'DD/MM/YYYY')) AS dt_ini_filtro,
        NVL(TO_DATE(:data_final, 'DD/MM/YYYY'), TRUNC(SYSDATE)) AS dt_fim_filtro,
        NVL(TO_DATE(:data_limite_processamento, 'DD/MM/YYYY'), TRUNC(SYSDATE)) AS dt_corte
    FROM dual
),

RANKING_0000 AS (
    -- 2. Busca os arquivos do período e aplica a regra de versionamento
    SELECT
        r.id AS reg_0000_id,
        r.cnpj,
        r.cod_fin,
        r.dt_ini,
        r.data_entrega,
        ROW_NUMBER() OVER (
            PARTITION BY r.cnpj, r.dt_ini 
            ORDER BY r.data_entrega DESC, r.id DESC
        ) AS rn
    FROM sped.reg_0000 r
    INNER JOIN PARAMETROS p ON r.cnpj = p.cnpj_filtro
    WHERE r.data_entrega <= p.dt_corte
      AND r.dt_ini BETWEEN p.dt_ini_filtro AND p.dt_fim_filtro
),

ARQUIVOS_VALIDOS AS (
    -- 3. Isola APENAS os IDs dos arquivos mais recentes
    SELECT reg_0000_id, dt_ini, cod_fin, data_entrega
    FROM RANKING_0000
    WHERE rn = 1
),

DADOS_0200 AS (
    -- 4. Traz TODAS as colunas da 0200 apenas para os arquivos válidos
    SELECT 
        r200.* FROM sped.reg_0200 r200
    INNER JOIN ARQUIVOS_VALIDOS av ON r200.reg_0000_id = av.reg_0000_id
),

DADOS_0205 AS (
    -- 5. CTE Modular para a REG_0205 (Alterações de Item)
    SELECT 
        r205.descr_ant_item,
        r205.dt_fim,
        r205.dt_ini,
        r205.cod_ant_item,
        r205.reg_0000_id,
        r205.reg_0200_id
    FROM sped.reg_0205 r205
    INNER JOIN ARQUIVOS_VALIDOS av ON r205.reg_0000_id = av.reg_0000_id
),

DADOS_0220 AS (
    -- 6. CTE Modular para a REG_0220 (Fatores de Conversão)
    SELECT 
        r220.reg_0000_id,
        r220.reg_0200_id,
        r220.unid_conv,
        r220.fat_conv
    FROM sped.reg_0220 r220
    INNER JOIN ARQUIVOS_VALIDOS av ON r220.reg_0000_id = av.reg_0000_id
)

-- 7. UNIÃO FINAL
SELECT
    TO_CHAR(arq.dt_ini, 'MM/YYYY') AS periodo_efd,
    
    -- Traz todas as colunas da CTE DADOS_0200
    r200.cod_item,
    r200.cod_ant_item,
    r205.cod_ant_item AS r0205_cod_ant_item,
    r200.descr_item,
    r200.aliq_icms,
    r200.unid_inv,
    r205.descr_ant_item,
    
    r205.dt_ini AS dt_ini_ant_item,
    r205.dt_fim AS dt_fim_ant_item,
    r220.unid_conv,
    r220.fat_conv,
    r200.cod_barra,
    r200.cod_ncm,
    r200.cest,
    r200.tipo_item,
    CASE r200.tipo_item
        WHEN '00' THEN '00 - Mercadoria para Revenda'
        WHEN '01' THEN '01 - Matéria-prima'
        WHEN '02' THEN '02 - Embalagem'
        WHEN '03' THEN '03 - Produto em Processo'
        WHEN '04' THEN '04 - Produto Acabado'
        WHEN '05' THEN '05 - Subproduto'
        WHEN '06' THEN '06 - Produto Intermediário'
        WHEN '07' THEN '07 - Material de Uso e Consumo'
        WHEN '08' THEN '08 - Ativo Imobilizado'
        WHEN '09' THEN '09 - Serviços'
        WHEN '10' THEN '10 - Outros insumos'
        WHEN '99' THEN '99 - Outras'
        ELSE r200.tipo_item
    END AS desc_tipo_item,
    r200.cod_gen,
    -- Decodificação do Gênero do Item (Capítulo NCM)
    CASE
        WHEN r200.cod_gen = '00' THEN 'Serviço'
        WHEN r200.cod_gen = '01' THEN 'Animais vivos'
        WHEN r200.cod_gen = '02' THEN 'Carnes e miudezas, comestíveis'
        WHEN r200.cod_gen = '03' THEN 'Peixes e crustáceos, moluscos e os outros invertebrados aquáticos'
        WHEN r200.cod_gen = '04' THEN 'Leite e laticínios, ovos de aves e outros produtos comestíveis de origem animal'
        WHEN r200.cod_gen = '05' THEN 'Outros produtos de origem animal'
        WHEN r200.cod_gen = '06' THEN 'Plantas vivas e produtos de floricultura'
        WHEN r200.cod_gen = '07' THEN 'Produtos hortícolas, raízes e tubérculos comestíveis'
        WHEN r200.cod_gen = '08' THEN 'Frutas, cascas de cítricos e de melões'
        WHEN r200.cod_gen = '09' THEN 'Café, chá, mate e especiarias'
        WHEN r200.cod_gen = '10' THEN 'Cereais'
        WHEN r200.cod_gen = '11' THEN 'Produtos da indústria de moagem'
        WHEN r200.cod_gen = '12' THEN 'Sementes e frutos oleaginosos'
        WHEN r200.cod_gen = '13' THEN 'Gomas, resinas e extratos vegetais'
        WHEN r200.cod_gen = '14' THEN 'Matérias para entrançar e outros produtos vegetais'
        WHEN r200.cod_gen = '15' THEN 'Gorduras de origem animal ou vegetal'
        WHEN r200.cod_gen = '16' THEN 'Preparações de carne e outros produtos de origem animal'
        WHEN r200.cod_gen = '17' THEN 'Açúcares e produtos de confeitaria'
        WHEN r200.cod_gen = '18' THEN 'Cacau e suas preparações'
        WHEN r200.cod_gen = '19' THEN 'Preparações à base de cereais e produtos de pastelaria'
        WHEN r200.cod_gen = '20' THEN 'Preparações de produtos hortícolas, frutas ou outras partes de plantas'
        WHEN r200.cod_gen = '21' THEN 'Preparações alimentícias diversas'
        WHEN r200.cod_gen = '22' THEN 'Bebidas, líquidos alcoólicos e vinagres'
        WHEN r200.cod_gen = '23' THEN 'Resíduos e desperdícios das indústrias alimentares'
        WHEN r200.cod_gen = '24' THEN 'Fumo (tabaco) e seus sucedâneos'
        WHEN r200.cod_gen = '25' THEN 'Sal, enxofre, terras e pedras'
        WHEN r200.cod_gen = '26' THEN 'Minérios, escórias e cinzas'
        WHEN r200.cod_gen = '27' THEN 'Combustíveis minerais e óleos minerais'
        WHEN r200.cod_gen = '28' THEN 'Produtos químicos inorgânicos'
        WHEN r200.cod_gen = '29' THEN 'Produtos químicos orgânicos'
        WHEN r200.cod_gen = '30' THEN 'Produtos farmacêuticos'
        WHEN r200.cod_gen = '31' THEN 'Adubos ou fertilizantes'
        WHEN r200.cod_gen = '32' THEN 'Extratos tanantes e tintas de escrever'
        WHEN r200.cod_gen = '33' THEN 'Óleos essenciais e resinoides'
        WHEN r200.cod_gen = '34' THEN 'Sabões, agentes orgânicos de superfície e produtos semelhantes'
        WHEN r200.cod_gen = '35' THEN 'Matérias albuminóides e enzimas'
        WHEN r200.cod_gen = '36' THEN 'Pólvoras e explosivos'
        WHEN r200.cod_gen = '37' THEN 'Produtos para fotografia e cinematografia'
        WHEN r200.cod_gen = '38' THEN 'Produtos diversos das indústrias químicas'
        WHEN r200.cod_gen = '39' THEN 'Plásticos e suas obras'
        WHEN r200.cod_gen = '40' THEN 'Borracha e suas obras'
        WHEN r200.cod_gen = '41' THEN 'Peles, exceto a peleteria'
        WHEN r200.cod_gen = '42' THEN 'Obras de couro'
        WHEN r200.cod_gen = '43' THEN 'Peleteria'
        WHEN r200.cod_gen = '44' THEN 'Madeira, carvão vegetal e obras de madeira'
        WHEN r200.cod_gen = '45' THEN 'Cortiça e suas obras'
        WHEN r200.cod_gen = '46' THEN 'Obras de espartaria ou cestaria'
        WHEN r200.cod_gen = '47' THEN 'Pastas de madeira e celulose'
        WHEN r200.cod_gen = '48' THEN 'Papel e cartão'
        WHEN r200.cod_gen = '49' THEN 'Livros, jornais e impressos'
        WHEN r200.cod_gen = '50' THEN 'Seda'
        WHEN r200.cod_gen = '51' THEN 'Lã e pelos finos'
        WHEN r200.cod_gen = '52' THEN 'Algodão'
        WHEN r200.cod_gen = '53' THEN 'Outras fibras têxteis vegetais'
        WHEN r200.cod_gen = '54' THEN 'Filamentos sintéticos ou artificiais'
        WHEN r200.cod_gen = '55' THEN 'Fibras sintéticas ou artificiais descontínuas'
        WHEN r200.cod_gen = '56' THEN 'Pastas ("ouates"), feltros e falsos tecidos'
        WHEN r200.cod_gen = '57' THEN 'Tapetes e outros revestimentos para pavimentos'
        WHEN r200.cod_gen = '58' THEN 'Tecidos especiais, tecidos tufados'
        WHEN r200.cod_gen = '59' THEN 'Tecidos impregnados'
        WHEN r200.cod_gen = '60' THEN 'Tecidos de malha'
        WHEN r200.cod_gen = '61' THEN 'Vestuário e acessórios de malha'
        WHEN r200.cod_gen = '62' THEN 'Vestuário e acessórios, exceto de malha'
        WHEN r200.cod_gen = '63' THEN 'Outros artefatos têxteis'
        WHEN r200.cod_gen = '64' THEN 'Calçados e artefatos semelhantes'
        WHEN r200.cod_gen = '65' THEN 'Chapéus e artefatos de uso semelhante'
        WHEN r200.cod_gen = '66' THEN 'Guarda-chuvas, bengalas e semelhantes'
        WHEN r200.cod_gen = '67' THEN 'Penas e penugem preparadas'
        WHEN r200.cod_gen = '68' THEN 'Produtos cerâmicos'
        WHEN r200.cod_gen = '69' THEN 'Obras de pedra'
        WHEN r200.cod_gen = '70' THEN 'Vidro e suas obras'
        WHEN r200.cod_gen = '71' THEN 'Pérolas naturais ou cultivadas'
        WHEN r200.cod_gen = '72' THEN 'Ferro fundido, ferro ou aço'
        WHEN r200.cod_gen = '73' THEN 'Obras de ferro fundido'
        WHEN r200.cod_gen = '74' THEN 'Cobre e suas obras'
        WHEN r200.cod_gen = '75' THEN 'Níquel e suas obras'
        WHEN r200.cod_gen = '76' THEN 'Alumínio e suas obras'
        WHEN r200.cod_gen = '77' THEN '[Reservado]'
        WHEN r200.cod_gen = '78' THEN 'Chumbo e suas obras'
        WHEN r200.cod_gen = '79' THEN 'Zinco e suas obras'
        WHEN r200.cod_gen = '80' THEN 'Estanho e suas obras'
        WHEN r200.cod_gen = '81' THEN 'Outros metais comuns'
        WHEN r200.cod_gen = '82' THEN 'Ferramentas e utensílios'
        WHEN r200.cod_gen = '83' THEN 'Obras diversas de metais comuns'
        WHEN r200.cod_gen = '84' THEN 'Máquinas, aparelhos e material elétrico'
        WHEN r200.cod_gen = '85' THEN 'Veículos automóveis'
        WHEN r200.cod_gen = '86' THEN 'Aeronaves e aparelhos espaciais'
        WHEN r200.cod_gen = '87' THEN 'Instrumentos e aparelhos'
        WHEN r200.cod_gen = '88' THEN 'Embarcações e estruturas flutuantes'
        WHEN r200.cod_gen = '89' THEN 'Instrumentos musicais'
        WHEN r200.cod_gen = '90' THEN 'Instrumentos e aparelhos ópticos'
        WHEN r200.cod_gen = '91' THEN 'Aparelhos de relojoaria'
        WHEN r200.cod_gen = '92' THEN 'Armas e munições'
        WHEN r200.cod_gen = '93' THEN 'Brinquedos'
        WHEN r200.cod_gen = '94' THEN 'Móveis'
        WHEN r200.cod_gen = '95' THEN 'Obras de arte e coleções'
        WHEN r200.cod_gen = '96' THEN 'Diversos'
        WHEN r200.cod_gen = '97' THEN 'Reservado'
        WHEN r200.cod_gen = '98' THEN 'Operações especiais'
        WHEN r200.cod_gen = '99' THEN 'Outros Diversos'
        ELSE 'Código Desconhecido'
    END AS descricao_cod_gen,

    
    arq.cod_fin AS cod_fin_efd,
    arq.data_entrega AS data_entrega_efd_periodo

FROM ARQUIVOS_VALIDOS arq
INNER JOIN DADOS_0200 r200 
    ON arq.reg_0000_id = r200.reg_0000_id
LEFT JOIN DADOS_0205 r205 
    ON r205.reg_0000_id = r200.reg_0000_id 
   AND r205.reg_0200_id = r200.id
LEFT JOIN DADOS_0220 r220 
    ON r220.reg_0000_id = r200.reg_0000_id 
   AND r220.reg_0200_id = r200.id
ORDER BY arq.dt_ini ASC;