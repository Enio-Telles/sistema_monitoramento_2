"""
Gera tabela_itens_caracteristicas_<cnpj>.parquet a partir dos parquets:
  - NFe_<cnpj>.parquet
  - NFCe_<cnpj>.parquet
  - c170_simplificada_<cnpj>.parquet   (ou c170_<cnpj>.parquet)
  - bloco_h_<cnpj>.parquet

Saída: CNPJ/<cnpj>/analises/tabela_itens_caracteristicas_<cnpj>.parquet

Colunas de saída:
  chave_item_individualizado, codigo, descricao, descr_compl,
  tipo_item, ncm, cest, gtin, lista_unidades, fonte
"""

import sys
import hashlib
from pathlib import Path

import polars as pl
from rich import print as rprint

# ──────────────────────────────────────────────
# Paths
# ──────────────────────────────────────────────
FUNCOES_DIR     = Path(r"c:\funcoes")
AUXILIARES_DIR  = FUNCOES_DIR / "funcoes_auxiliares"
REFS_DIR        = FUNCOES_DIR / "referencias" / "CO_SEFIN"

if str(AUXILIARES_DIR) not in sys.path:
    sys.path.insert(0, str(AUXILIARES_DIR))

try:
    from salvar_para_parquet import salvar_para_parquet
    from validar_cnpj import validar_cnpj
    from encontrar_arquivo_cnpj import encontrar_arquivo
    # Importa a função de inferência do arquivo co_sefin.py no mesmo diretório
    from co_sefin import co_sefin
    from aux_leitura_notas import ler_nfe_nfce, ler_c170
except ImportError as e:
    rprint(f"[red]Erro ao importar módulos auxiliares:[/red] {e}")
    sys.exit(1)


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────
CAMPOS_CHAVE = ["codigo", "descricao", "descr_compl", "tipo_item", "ncm", "cest", "gtin"]




def _normalizar(df: pl.DataFrame) -> pl.DataFrame:
    """Garante que todos os campos chave existam no DataFrame (preenche com null se ausentes)."""
    for col in CAMPOS_CHAVE + ["unidade", "fonte"]:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.String).alias(col))
    return df


def _gerar_chave(df: pl.DataFrame) -> pl.DataFrame:
    """Adiciona a coluna chave_item_individualizado usando hash nativo."""
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
          .hash(seed=42)
          .cast(pl.String)
          .str.encode("hex")
          .alias("chave_item_individualizado")
    ).drop(key_cols)

    return df



def _aplicar_normalizacao(df: pl.DataFrame) -> pl.DataFrame:
    """
    Retorna uma cópia do DataFrame com campos de texto normalizados:
      - Remove acentos e cedilha (c -> c)
      - Converte para maiúsculas
      - Remove espaços antes e depois
      - codigo: remove zeros à esquerda (preserva letras)
      - ncm, cest, gtin: remove pontos
    Campos do tipo lista (lista_unidades, fonte) têm seus elementos normalizados individualmente.
    """
    import unicodedata

    def _norm(v):
        if v is None:
            return None
        v = unicodedata.normalize("NFD", str(v))
        v = "".join(c for c in v if unicodedata.category(c) != "Mn")
        return v.upper().strip()

    def _norm_codigo(v):
        """Acento + upper + strip + remove zeros à esquerda (preserva letras)."""
        if v is None:
            return None
        v = _norm(v)
        return v.lstrip("0") or "0"

    def _remove_pontos(v):
        """Acento + upper + strip + remove pontos."""
        if v is None:
            return None
        return _norm(v).replace(".", "")

    COLS_TEXTO  = ["descricao", "descr_compl", "tipo_item"]
    COLS_PONTOS = ["ncm", "cest", "gtin"]
    COLS_LISTA  = ["lista_unidades", "fonte"]

    exprs = []

    # Campos de texto genérico
    for col in COLS_TEXTO:
        if col in df.columns:
            exprs.append(pl.col(col).map_elements(_norm, return_dtype=pl.String).alias(col))

    # cod_normalizado: sem zeros à esquerda (mantendo o codigo original)
    if "codigo" in df.columns:
        exprs.append(pl.col("codigo").map_elements(_norm_codigo, return_dtype=pl.String).alias("cod_normalizado"))

    # ncm, cest, gtin: sem pontos
    for col in COLS_PONTOS:
        if col in df.columns:
            exprs.append(pl.col(col).map_elements(_remove_pontos, return_dtype=pl.String).alias(col))

    # Listas
    for col in COLS_LISTA:
        if col in df.columns:
            exprs.append(
                pl.col(col).list.eval(
                    pl.element().map_elements(_norm, return_dtype=pl.String)
                ).alias(col)
            )

    if exprs:
        df = df.with_columns(exprs)

    # Reordenar colunas: colocar cod_normalizado ao lado de codigo
    if "cod_normalizado" in df.columns and "codigo" in df.columns:
        cols = df.columns
        cols.remove("cod_normalizado")
        idx = cols.index("codigo")
        cols.insert(idx + 1, "cod_normalizado")
        df = df.select(cols)

    return df


# ──────────────────────────────────────────────
# Leitores por fonte (lazy → seleção mínima)
# ──────────────────────────────────────────────



def _ler_bloco_h(path: Path | None) -> pl.DataFrame | None:
    """Lê bloco_h e mapeia colunas do 0200 (que já vêm no parquet)."""
    if path is None or not path.exists():
        rprint("[yellow]  ⚠️  Bloco H não encontrado.[/yellow]")
        return None

    schema = pl.read_parquet(path, n_rows=0).schema

    # Mapeamento (nomes conforme gerado pelo bloco_h.sql)
    col_map = {}
    for destino, candidatos in {
        "codigo":    ["codigo_produto", "codigo_produto_original"],
        "descricao": ["descricao_produto"],
        "tipo_item": ["tipo_item"],
        "ncm":       ["cod_ncm"],
        "cest":      ["cest"],
        "gtin":      ["cod_barra"],
        "unidade":   ["unidade_medida"],
    }.items():
        for c in candidatos:
            if c in schema:
                col_map[c] = destino
                break

    selecionar = list(col_map.keys())
    df = (
        pl.scan_parquet(path)
        .select(selecionar)
        .collect()
        .rename(col_map)
    )

    if df.is_empty():
        return None

    df = df.with_columns(pl.lit(None, pl.String).alias("descr_compl"))

    rprint(f"[green]  Bloco H: {len(df):,} linhas[/green]")
    return df


# ──────────────────────────────────────────────
# Função principal
# ──────────────────────────────────────────────

def gerar_tabela_itens_caracteristicas(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    """
    Gera tabela_itens_caracteristicas_<cnpj>.parquet consolidando NFe, NFCe, C170 e Bloco H.

    Args:
        cnpj: CNPJ numérico (14 dígitos).
        pasta_cnpj: Pasta raiz do CNPJ (padrão: c:/funcoes/CNPJ/<cnpj>).

    Returns:
        True se gerado com sucesso, False caso contrário.
    """
    import re
    cnpj = re.sub(r"[^0-9]", "", cnpj)

    if pasta_cnpj is None:
        pasta_cnpj = FUNCOES_DIR / "CNPJ" / cnpj

    arq_dir = pasta_cnpj / "arquivos_parquet"

    rprint(f"\n[bold cyan]Gerando tabela_itens_caracteristicas para CNPJ: {cnpj}[/bold cyan]")

    # ── 1. Localiza arquivos via encontrar_arquivo ──
    diretorios_validos = [d for d in (arq_dir, pasta_cnpj) if d.exists()]

    def _resolver(prefixo: str) -> Path | None:
        """Busca o parquet em arquivos_parquet/ e depois na raiz do CNPJ."""
        for diretorio in diretorios_validos:
            arq = encontrar_arquivo(diretorio, prefixo, cnpj)
            if arq:
                return arq
        return None

    # ── 2. Carrega ano base do reg_0000 ──
    ano_base = ""
    reg_0000_path = _resolver("reg_0000")
    if reg_0000_path:
        try:
            df_0000 = pl.read_parquet(reg_0000_path, n_rows=1)
            if "dt_ini" in df_0000.columns:
                ano_base = str(df_0000["dt_ini"][0])[0:4]
            elif "dt_periodo" in df_0000.columns:
                ano_base = str(df_0000["dt_periodo"][0])[0:4]
        except:
            pass

    # ── 3. Carrega CFOP BI para filtro de Operação Mercantil 'X' ──
    cfop_bi_path = Path(r"c:\funcoes\referencias\cfop\cfop_bi.parquet")
    cfop_df = None
    if cfop_bi_path.exists():
        cfop_df = (
            pl.scan_parquet(cfop_bi_path)
            .filter(pl.col("operacao_mercantil") == "X")
            .select(["co_cfop"])
            .collect()
            .with_columns(pl.col("co_cfop").cast(pl.String))
        )
    else:
        rprint("[yellow]  ⚠️  cfop_bi.parquet não encontrado. Filtro de Operação Mercantil 'X' não será aplicado.[/yellow]")

    # ── 4. Leitura e padronização por fonte ──────────────
    fragmentos: list[pl.DataFrame] = []

    for nome_fonte, df_src in [
        ("NFe",    ler_nfe_nfce(_resolver("NFe"),  cnpj, "NFe", cfop_df, print_status=True)),
        ("NFCe",   ler_nfe_nfce(_resolver("NFCe"), cnpj, "NFCe", cfop_df, print_status=True)),
        ("C170",   ler_c170(_resolver("c170_simplificada") or _resolver("c170"), cfop_df, ano_base, print_status=True)),
        ("bloco_h", _ler_bloco_h(_resolver("bloco_h"))),
    ]:
        if df_src is not None and not df_src.is_empty():
            # Marca a origem de cada linha
            df_src = df_src.with_columns(pl.lit(nome_fonte, pl.String).alias("fonte"))
            df_src = _normalizar(df_src)
            # Garante que só as colunas de interesse sobrevivam
            cols_finais = CAMPOS_CHAVE + ["unidade", "fonte", "valor_entrada", "valor_saida", 
                                          "quantidade_entrada", "quantidade_saida", "ano"]
            df_src = df_src.select([c for c in cols_finais if c in df_src.columns])
            df_src = _normalizar(df_src)
            fragmentos.append(df_src)

    if not fragmentos:
        rprint("[red]❌ Nenhuma fonte disponível. Abortando.[/red]")
        return False

    # ── 5. Empilha todas as fontes ───────────────────────
    df_total = pl.concat(fragmentos, how="diagonal_relaxed")
    # Garante que colunas de valor e quantidade existam
    for col in ["valor_entrada", "valor_saida", "quantidade_entrada", "quantidade_saida"]:
        if col not in df_total.columns:
            df_total = df_total.with_columns(pl.lit(0.0).alias(col))
        else:
            df_total = df_total.with_columns(pl.col(col).fill_null(0).cast(pl.Float64))
    
    if "ano" not in df_total.columns:
        df_total = df_total.with_columns(pl.lit(ano_base).alias("ano"))
    else:
        df_total = df_total.with_columns(pl.col("ano").fill_null(ano_base).cast(pl.String))

    rprint(f"[cyan]  Total de linhas empilhadas: {len(df_total):,}[/cyan]")

    # ── 6. Gera chave MD5 ────────────────────────────────
    df_total = _gerar_chave(df_total)

    # ── 7. Deduplicação: agrupa por chave, consolida unidades e totais ──
    df_resultado = (
        df_total
        .group_by("chave_item_individualizado")
        .agg(
            # Para campos fixos: primeiro valor não-nulo
            *[
                pl.col(c).drop_nulls().first().alias(c)
                for c in CAMPOS_CHAVE
            ],
            # Totais financeiros e quantidades
            pl.col("valor_entrada").sum().alias("total_entradas"),
            pl.col("valor_saida").sum().alias("total_saidas"),
            pl.col("quantidade_entrada").sum().alias("qtd_entradas"),
            pl.col("quantidade_saida").sum().alias("qtd_saidas"),
            # Lista de unidades únicas e ordenadas
            pl.col("unidade")
              .filter(pl.col("unidade").is_not_null() & (pl.col("unidade") != ""))
              .unique()
              .sort()
              .alias("lista_unidades"),
            # Fontes de origem do item (lista ordenada e deduplicada)
            pl.col("fonte")
              .drop_nulls()
              .unique()
              .sort()
              .alias("fonte"),
        )
        # Ordena por descricao → codigo para leitura facilitada (item_1 será o primeiro nome A-Z)
        .sort(["descricao", "codigo"], nulls_last=True)
        # Substitui o hash MD5 por um ID sequencial item_1, item_2...
        .with_columns(
            (pl.lit("item_") + pl.int_range(1, pl.len() + 1).cast(pl.String))
            .alias("chave_item_individualizado")
        )
    )

    rprint(f"[bold green]  {len(df_resultado):,} itens únicos encontrados.[/bold green]")

    # ── 6. Salva tabela original ─────────────────────────
    pasta_saida = pasta_cnpj / "analises" / "produtos"
    nome_original = f"tabela_itens_caracteristicas_{cnpj}.parquet"
    ok = salvar_para_parquet(df_resultado, pasta_saida, nome_original)

    # ── 7. Gera e salva tabela normalizada ───────────────
    rprint("[cyan]  Gerando versão normalizada (sem acentos, maiúsculo)...[/cyan]")
    df_normalizado = _aplicar_normalizacao(df_resultado)
    nome_normalizado = f"tab_itens_caract_normalizada_{cnpj}.parquet"
    ok_norm = salvar_para_parquet(df_normalizado, pasta_saida, nome_normalizado)

    # ── 8. Inferência de SEFIN (co_sefin_inferido) ────────
    # Chama a função importada do arquivo co_sefin.py
    # Ela irá ler os parquets que acabamos de salvar e injetar a coluna co_sefin_inferido
    rprint("[cyan]  Iniciando inferência de SEFIN via co_sefin.py...[/cyan]")
    ok_sefin = co_sefin(cnpj, pasta_cnpj)

    return ok and ok_norm and ok_sefin


# ──────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────
if __name__ == "__main__":
    import re

    if len(sys.argv) > 1:
        cnpj_arg = sys.argv[1]
    else:
        try:
            cnpj_arg = input("Informe o CNPJ: ").strip()
        except (KeyboardInterrupt, EOFError):
            rprint("\n[yellow]Cancelado.[/yellow]")
            sys.exit(0)

    cnpj_arg = re.sub(r"[^0-9]", "", cnpj_arg)

    if not validar_cnpj(cnpj_arg):
        rprint(f"[red]CNPJ inválido: {cnpj_arg}[/red]")
        sys.exit(1)

    sucesso = gerar_tabela_itens_caracteristicas(cnpj_arg)
    sys.exit(0 if sucesso else 1)
