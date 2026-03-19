"""
fator_conversao.py

Script para calcular o fator de conversão de unidades para cada produto anualmente,
baseado na média de preços das entradas (C170) e saídas (NFe/NFCe).
"""

import sys
from pathlib import Path
from collections import Counter

import polars as pl
from rich import print as rprint

FUNCOES_DIR = Path(r"c:\funcoes")
AUXILIARES_DIR = FUNCOES_DIR / "funcoes_auxiliares"
TABELA_PRODUTOS_DIR = FUNCOES_DIR / "funcoes_tabelas" / "tabela_produtos"

if str(AUXILIARES_DIR) not in sys.path:
    sys.path.insert(0, str(AUXILIARES_DIR))

try:
    from salvar_para_parquet import salvar_para_parquet
    from validar_cnpj import validar_cnpj
    from encontrar_arquivo_cnpj import encontrar_arquivo
    from aux_leitura_notas import ler_nfe_nfce, ler_c170
except ImportError as e:
    rprint(f"[red]Erro ao importar módulos auxiliares:[/red] {e}")
    sys.exit(1)


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────
CAMPOS_CHAVE = ["codigo", "descricao", "descr_compl", "tipo_item", "ncm", "cest", "gtin"]


def _md5_row(values: list) -> str:
    """Gera um hash MD5 a partir de uma lista de valores (normalizados)."""
    import hashlib
    partes = [str(v).strip().upper() if v is not None else "" for v in values]
    return hashlib.md5("|".join(partes).encode("utf-8")).hexdigest()


def _normalizar(df: pl.DataFrame) -> pl.DataFrame:
    """Garante que todos os campos chave existam no DataFrame (preenche com null se ausentes)."""
    for col in CAMPOS_CHAVE + ["unidade", "fonte"]:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.String).alias(col))
    return df


def _gerar_chave(df: pl.DataFrame) -> pl.DataFrame:
    """Adiciona a coluna chave_item_individualizado (MD5 dos campos chave)."""
    # Normaliza cada campo chave para string uppercase sem espaços laterais
    exprs_norm = [
        pl.when(pl.col(c).is_null())
          .then(pl.lit(""))
          .otherwise(pl.col(c).cast(pl.String).str.strip_chars().str.to_uppercase())
          .alias(f"_key_{c}")
        for c in CAMPOS_CHAVE
    ]
    df = df.with_columns(exprs_norm)

    key_cols = [f"_key_{c}" for c in CAMPOS_CHAVE]

    df = df.with_columns(
        pl.concat_str(key_cols, separator="|")
          .map_elements(_md5_row, return_dtype=pl.String)
          .alias("chave_item_individualizado")
    ).drop(key_cols)

    return df





def gerar_template_fatores_manuais(pasta_saida: Path) -> bool:
    """Gera um template Excel vazio para insercao manual de fatores de conversao."""
    cols = ["chave_produto", "codigo", "descricao", "unidade", "ano", "fator_conversao_manual", "justificativa"]
    df_template = pl.DataFrame(schema={c: pl.String for c in cols})
    arquivo_saida = pasta_saida / "template_fatores_manuais.xlsx"
    try:
        df_template.write_excel(arquivo_saida)
        rprint(f"[green]Template para fatores manuais gerado em: {arquivo_saida}[/green]")
        return True
    except Exception as e:
        rprint(f"[red]Erro ao gerar template Excel: {e}[/red]")
        return False


def ler_fatores_manuais(arquivo_excel: Path) -> pl.DataFrame | None:
    """Lê a planilha de fatores de conversão manuais, se existir."""
    if not arquivo_excel.exists():
        return None
    try:
        df_manual = pl.read_excel(arquivo_excel)
        # Garantir colunas essenciais
        cols_essenciais = ["chave_produto", "unidade", "ano", "fator_conversao_manual"]
        for col in cols_essenciais:
            if col not in df_manual.columns:
                rprint(f"[red]Planilha manual não contém a coluna obrigatória: {col}[/red]")
                return None

        # Tipar e filtrar nulos
        df_manual = df_manual.with_columns([
            pl.col("chave_produto").cast(pl.String),
            pl.col("unidade").cast(pl.String),
            pl.col("ano").cast(pl.String),
            pl.col("fator_conversao_manual").cast(pl.Float64)
        ]).drop_nulls(subset=["fator_conversao_manual", "chave_produto", "unidade", "ano"])

        return df_manual
    except Exception as e:
        rprint(f"[red]Erro ao ler planilha de fatores manuais ({arquivo_excel}): {e}[/red]")
        return None

def _agrupar_por_produto_ano(df_vols: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Agrupa os volumes por produto, unidade e ano para calcular médias e definir unidade padrão."""
    # 5. Agrupar por (chave_produto, unidade, ano) para calcular médias
    df_aggr = (
        df_vols
        .group_by(["chave_produto", "unidade", "ano", "unid_padrao_escolhida"])
        .agg([
            pl.col("valor_entrada").sum().alias("v_ent"),
            pl.col("quantidade_entrada").sum().alias("q_ent"),
            pl.col("valor_saida").sum().alias("v_sai"),
            pl.col("quantidade_saida").sum().alias("q_sai"),
            pl.len().alias("ocorrencias")
        ])
        .with_columns([
            (pl.col("v_ent") / pl.col("q_ent")).fill_nan(0).alias("preco_med_ent"),
            (pl.col("v_sai") / pl.col("q_sai")).fill_nan(0).alias("preco_med_sai"),
        ])
    )

    # 6. Unidade Padrão: usar a escolhida na tabela de descrições, ou a moda se vazia
    df_unid_padrao_auto = (
        df_aggr
        .group_by(["chave_produto", "ano"])
        .agg(pl.col("unidade").sort_by("ocorrencias", descending=True).first().alias("unid_padrao_auto"))
    )

    df_fator_pre = df_aggr.join(df_unid_padrao_auto, on=["chave_produto", "ano"])

    df_fator = df_fator_pre.with_columns(
        pl.coalesce(["unid_padrao_escolhida", "unid_padrao_auto"]).alias("unid_padrao")
    )
    return df_aggr, df_fator


def _calcular_fator_final(df_fator: pl.DataFrame, df_aggr: pl.DataFrame) -> pl.DataFrame:
    """Calcula o fator de conversão em relação à unidade padrão escolhida/calculada."""
    # 7. Join para calcular fator em relação à unid_padrao
    df_precos_padrao = (
        df_aggr
        .select(["chave_produto", "ano", "unidade", "preco_med_ent", "preco_med_sai"])
        .rename({"unidade": "unid_padrao", "preco_med_ent": "preco_padrao_ent", "preco_med_sai": "preco_padrao_sai"})
    )

    df_final = (
        df_fator.join(df_precos_padrao, on=["chave_produto", "ano", "unid_padrao"], how="left")
        .with_columns([
            # Cálculo do fator
            pl.when(pl.col("preco_padrao_ent") > 0)
              .then(pl.col("preco_med_ent") / pl.col("preco_padrao_ent"))
              .when((pl.col("preco_padrao_ent").is_null() | (pl.col("preco_padrao_ent") == 0)) & (pl.col("preco_padrao_sai") > 0))
              .then(pl.col("preco_med_sai") / pl.col("preco_padrao_sai"))
              .otherwise(0.0)
              .alias("fator_conversao")
        ])
        .select([
            "chave_produto", "ano", "unidade", "unid_padrao",
            "v_ent", "q_ent", "preco_med_ent",
            "v_sai", "q_sai", "preco_med_sai",
            "fator_conversao"
        ])
        .sort(["chave_produto", "ano", "unidade"])
    )

    return df_final


def calcular_fator_conversao(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    import re
    cnpj = re.sub(r"[^0-9]", "", cnpj)

    if pasta_cnpj is None:
        pasta_cnpj = FUNCOES_DIR / "CNPJ" / cnpj

    pasta_produtos = pasta_cnpj / "analises" / "produtos"
    # Precisamos da tabela_descricoes para saber a chave_produto de cada item
    arq_descricoes = pasta_produtos / f"tabela_descricoes_{cnpj}.parquet"
    # Precisamos da tabela_itens_caracteristicas (original antes da aggr final se possível, 
    # ou reconstruir a partir das fontes com a mesma lógica)
    
    if not arq_descricoes.exists():
        rprint(f"[red]Erro: Tabela descrições não encontrada: {arq_descricoes}[/red]")
        return False

    rprint(f"\n[bold cyan]Calculando fator_conversao para CNPJ: {cnpj}[/bold cyan]")

    # 1. Carregar mapeamento de itens para chave_produto e unidade padrão escolhida
    df_desc = pl.read_parquet(arq_descricoes)
    
    # Mapeamento do item para chave_produto e unid_padrao do produto
    df_item_prod = (
        df_desc.select(["chave_produto", "unid_padrao", "lista_chave_item_individualizado"])
        .explode("lista_chave_item_individualizado")
        .rename({"lista_chave_item_individualizado": "chave_item_individualizado", "unid_padrao": "unid_padrao_escolhida"})
    )

    # 2. Precisamos dos itens originais com (chave_item, unidade, valor, qtd, ano)
    # Re-ler fontes com foco em valores e quantidades para cálculo de média
    
    cfop_bi_path = Path(r"c:\funcoes\referencias\cfop\cfop_bi.parquet")
    cfop_df = None
    if cfop_bi_path.exists():
        cfop_df = (
            pl.read_parquet(cfop_bi_path)
            .filter(pl.col("operacao_mercantil") == "X")
            .select(["co_cfop"])
            .with_columns(pl.col("co_cfop").cast(pl.String))
        )

    # Ano base (EFD)
    ano_base = ""
    arq_dir = pasta_cnpj / "arquivos_parquet"
    
    dirs_validos = [d for d in (arq_dir, pasta_cnpj) if d.exists()]
    def resolver_local(prefixo):
        for d in dirs_validos:
            a = encontrar_arquivo(d, prefixo, cnpj)
            if a: return a
        return None
        
    reg_0000_path = resolver_local("reg_0000")
    if reg_0000_path:
        try:
            df_0000 = pl.read_parquet(reg_0000_path, n_rows=1)
            if "dt_ini" in df_0000.columns: ano_base = str(df_0000["dt_ini"][0])[0:4]
        except: pass

    fragmentos = []
    for nome, df_src in [
        ("NFe",  ler_nfe_nfce(resolver_local("NFe"), cnpj, "NFe", cfop_df)),
        ("NFCe", ler_nfe_nfce(resolver_local("NFCe"), cnpj, "NFCe", cfop_df)),
        ("C170", ler_c170(resolver_local("c170_simplificada") or resolver_local("c170"), cfop_df, ano_base))
    ]:
        if df_src is not None:
            cols = CAMPOS_CHAVE + ["unidade", "valor_entrada", "valor_saida", "quantidade_entrada", "quantidade_saida", "ano"]
            df_src = df_src.select([c for c in cols if c in df_src.columns])
            fragmentos.append(df_src)

    if not fragmentos:
        rprint("[red]❌ Nenhuma fonte de valores encontrada p/ fator conversão.[/red]")
        return False

    df_total = pl.concat(fragmentos, how="diagonal_relaxed")
    df_total = _gerar_chave(df_total) # chave_item_individualizado
    
    # 4. Join com chave_produto e unid_padrao_escolhida
    df_vols = df_total.join(df_item_prod, on="chave_item_individualizado", how="inner")

    # 5 e 6. Agrupar por produto/ano e definir unidade padrão
    df_aggr, df_fator = _agrupar_por_produto_ano(df_vols)

    # 7. Join para calcular fator em relação à unid_padrao
    df_final = _calcular_fator_final(df_fator, df_aggr)

    # 7.1. Mesclar com fatores manuais (se houver)
    arquivo_manual = pasta_produtos / f"fatores_manuais_{cnpj}.xlsx"
    df_manual = ler_fatores_manuais(arquivo_manual)

    if df_manual is not None:
        rprint(f"[yellow]Aplicando fatores de conversão manuais encontrados em: {arquivo_manual.name}[/yellow]")
        # Garantir mesmo tipo de dados para join
        df_manual = df_manual.select(["chave_produto", "unidade", "ano", "fator_conversao_manual"])
        df_final = df_final.join(df_manual, on=["chave_produto", "unidade", "ano"], how="left")
        df_final = df_final.with_columns([
            pl.when(pl.col("fator_conversao_manual").is_not_null())
              .then(pl.col("fator_conversao_manual"))
              .otherwise(pl.col("fator_conversao"))
              .alias("fator_conversao"),
            pl.when(pl.col("fator_conversao_manual").is_not_null())
              .then(pl.lit("manual"))
              .otherwise(pl.lit("automático"))
              .alias("fonte_fator")
        ]).drop("fator_conversao_manual")
    else:
        # Se não houver, tudo é automático
        df_final = df_final.with_columns(pl.lit("automático").alias("fonte_fator"))

    # 8. Salvar
    nome_saida = f"fator_conversao_{cnpj}.parquet"
    ok = salvar_para_parquet(df_final, pasta_produtos, nome_saida)
    
    # 9. Gerar template sempre
    gerar_template_fatores_manuais(pasta_produtos)

    return ok


if __name__ == "__main__":
    if len(sys.argv) > 1:
        calcular_fator_conversao(sys.argv[1])
    else:
        cnpj_input = input("CNPJ: ").strip()
        if cnpj_input:
            calcular_fator_conversao(cnpj_input)
