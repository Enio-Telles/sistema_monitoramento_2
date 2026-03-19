"""
tabela_descricoes_v2.py

Agrupa produtos por descricao_normalizada, preservando rastreabilidade dos itens
e preparando a base para consolidação posterior.
"""

import hashlib
import sys
from pathlib import Path

import polars as pl
from rich import print as rprint

FUNCOES_DIR = Path(r"c:\funcoes")
AUXILIARES_DIR = FUNCOES_DIR / "funcoes_auxiliares"

if str(AUXILIARES_DIR) not in sys.path:
    sys.path.insert(0, str(AUXILIARES_DIR))

try:
    from salvar_para_parquet import salvar_para_parquet
    from validar_cnpj import validar_cnpj
except ImportError as e:
    rprint(f"[red]Erro ao importar módulos auxiliares:[/red] {e}")
    sys.exit(1)


def _moda_texto(lista: list[str] | None) -> str | None:
    if not lista:
        return None
    limpos = [str(x).strip() for x in lista if x not in (None, "", []) and str(x).strip()]
    if not limpos:
        return None
    from collections import Counter
    cont = Counter(limpos)
    maior = max(cont.values())
    candidatos = sorted([k for k, v in cont.items() if v == maior])
    return candidatos[0] if candidatos else None


def _descricao_representativa(lista: list[str] | None) -> str | None:
    if not lista:
        return None
    limpos = [str(x).strip() for x in lista if x not in (None, "", []) and str(x).strip()]
    if not limpos:
        return None
    return sorted(limpos, key=lambda x: (-len(x.split()), x))[0]


def _gerar_chave_produto(lista_chaves: list[str] | None) -> str:
    if not lista_chaves:
        return ""
    texto = "".join(sorted([str(c) for c in lista_chaves]))
    return hashlib.md5(texto.encode()).hexdigest()


def gerar_tabela_descricoes_v2(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    import re
    cnpj = re.sub(r"[^0-9]", "", cnpj)

    if pasta_cnpj is None:
        pasta_cnpj = FUNCOES_DIR / "CNPJ" / cnpj

    pasta_produtos = pasta_cnpj / "analises" / "produtos"
    arq_entrada = pasta_produtos / f"tab_itens_caract_normalizada_v2_{cnpj}.parquet"
    if not arq_entrada.exists():
        arq_entrada = pasta_produtos / f"tab_itens_caract_normalizada_{cnpj}.parquet"

    if not arq_entrada.exists():
        rprint(f"[red]Erro: Arquivo normalizado não encontrado: {arq_entrada}[/red]")
        return False

    rprint(f"\n[bold cyan]Gerando tabela_descricoes_v2 para CNPJ: {cnpj}[/bold cyan]")

    df = pl.read_parquet(arq_entrada)
    cols = df.columns

    if "descricao_normalizada" not in cols and "descricao" in cols:
        df = df.with_columns(
            pl.col("descricao").cast(pl.String).str.to_uppercase().str.strip_chars().alias("descricao_normalizada")
        )
        cols = df.columns

    if "descricao_normalizada" not in cols:
        rprint("[red]Erro: coluna descricao_normalizada não encontrada.[/red]")
        return False

    agg_exprs = [
        pl.col("chave_item_individualizado").unique().sort().alias("lista_chave_item_individualizado"),
        pl.col("codigo").drop_nulls().unique().sort().alias("lista_codigos"),
        pl.col("descricao").drop_nulls().unique().sort().alias("lista_descricoes_originais"),
        pl.col("total_entradas").sum().alias("total_entradas"),
        pl.col("total_saidas").sum().alias("total_saidas"),
        pl.col("qtd_entradas").sum().alias("qtd_entradas"),
        pl.col("qtd_saidas").sum().alias("qtd_saidas"),
        pl.len().alias("qtd_itens_origem"),
    ]

    mapeamento_listas = {
        "descr_compl": "lista_descr_compl",
        "tipo_item": "lista_tipo_item",
        "ncm": "lista_ncm",
        "cest": "lista_cest",
        "gtin": "lista_gtin",
        "co_sefin_inferido": "lista_co_sefin_inferido",
        "item_seq_id": "lista_item_seq_id",
    }

    for orig, dest in mapeamento_listas.items():
        if orig in cols:
            agg_exprs.append(pl.col(orig).drop_nulls().unique().sort().alias(dest))

    if "lista_unidades" in cols:
        agg_exprs.append(pl.col("lista_unidades").flatten().drop_nulls().unique().sort().alias("lista_unids"))
    elif "unidade" in cols:
        agg_exprs.append(pl.col("unidade").drop_nulls().unique().sort().alias("lista_unids"))

    if "fonte" in cols:
        agg_exprs.append(pl.col("fonte").flatten().drop_nulls().unique().sort().alias("lista_fonte"))

    df_resultado = (
        df.group_by("descricao_normalizada")
        .agg(agg_exprs)
        .with_columns([
            pl.col("lista_descricoes_originais").map_elements(_descricao_representativa, return_dtype=pl.Utf8).alias("descricao_representativa"),
            pl.col("lista_ncm").map_elements(_moda_texto, return_dtype=pl.Utf8).alias("ncm_padrao"),
            pl.col("lista_cest").map_elements(_moda_texto, return_dtype=pl.Utf8).alias("cest_padrao"),
            pl.col("lista_gtin").map_elements(_moda_texto, return_dtype=pl.Utf8).alias("gtin_padrao"),
            pl.col("lista_tipo_item").map_elements(_moda_texto, return_dtype=pl.Utf8).alias("tipo_item_padrao"),
            pl.col("lista_unids").map_elements(_moda_texto, return_dtype=pl.Utf8).alias("unid_padrao"),
            pl.col("lista_chave_item_individualizado").map_elements(_gerar_chave_produto, return_dtype=pl.Utf8).alias("chave_produto"),
            pl.col("lista_codigos").map_elements(_moda_texto, return_dtype=pl.Utf8).alias("codigo_padrao"),
            pl.lit(False).alias("verificado"),
        ])
        .with_columns([
            pl.when(pl.col("lista_co_sefin_inferido").is_not_null())
              .then(pl.col("lista_co_sefin_inferido").map_elements(_moda_texto, return_dtype=pl.Utf8))
              .otherwise(pl.lit(None, dtype=pl.Utf8))
              .alias("co_sefin_agr"),
            pl.when(pl.col("lista_co_sefin_inferido").is_not_null())
              .then(pl.col("lista_co_sefin_inferido").list.unique().list.len().gt(1))
              .otherwise(pl.lit(False))
              .alias("co_sefin_agr_divergente"),
            (
                (pl.col("qtd_itens_origem") / (pl.col("qtd_itens_origem") + 1.0))
                .cast(pl.Float64)
            ).alias("score_consistencia")
        ])
        .sort("descricao_normalizada")
    )

    nome_saida = f"tabela_descricoes_v2_{cnpj}.parquet"
    ok = salvar_para_parquet(df_resultado, pasta_produtos, nome_saida)
    return ok


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

    sucesso = gerar_tabela_descricoes_v2(cnpj_arg)
    sys.exit(0 if sucesso else 1)
