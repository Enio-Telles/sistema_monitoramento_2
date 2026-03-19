"""
produtos_consolidados.py

Consolida a tabela de descrições em uma tabela oficial de produtos,
com foco em rastreabilidade, revisão e uso como base para conversão.
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


def _gerar_chave_id(lista_chaves_produto: list[str] | None) -> str:
    if not lista_chaves_produto:
        return ""
    texto = "".join(sorted([str(x) for x in lista_chaves_produto]))
    return hashlib.md5(texto.encode()).hexdigest()


def gerar_produtos_consolidados(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    import re
    cnpj = re.sub(r"[^0-9]", "", cnpj)

    if pasta_cnpj is None:
        pasta_cnpj = FUNCOES_DIR / "CNPJ" / cnpj

    pasta_produtos = pasta_cnpj / "analises" / "produtos"
    arq_descricoes = pasta_produtos / f"tabela_descricoes_v2_{cnpj}.parquet"
    if not arq_descricoes.exists():
        arq_descricoes = pasta_produtos / f"tabela_descricoes_{cnpj}.parquet"

    if not arq_descricoes.exists():
        rprint(f"[red]Erro: tabela de descrições não encontrada: {arq_descricoes}[/red]")
        return False

    df = pl.read_parquet(arq_descricoes)

    if "descricao_representativa" not in df.columns and "descricao" in df.columns:
        df = df.with_columns(pl.col("descricao").alias("descricao_representativa"))

    if "descricao_representativa" not in df.columns and "descricao_normalizada" in df.columns:
        df = df.with_columns(pl.col("descricao_normalizada").alias("descricao_representativa"))

    if "lista_chave_item_individualizado" not in df.columns:
        rprint("[red]Erro: a tabela de descrições não possui lista_chave_item_individualizado.[/red]")
        return False

    if "lista_item_seq_id" not in df.columns:
        df = df.with_columns(pl.lit([], dtype=pl.List(pl.Utf8)).alias("lista_item_seq_id"))

    df_resultado = (
        df.with_columns([
            pl.col("chave_produto").map_elements(lambda x: [str(x)] if x not in (None, "") else [], return_dtype=pl.List(pl.Utf8)).alias("lista_chave_produto"),
            pl.col("chave_produto").map_elements(lambda x: [str(x)] if x not in (None, "") else [], return_dtype=pl.List(pl.Utf8)).alias("_tmp_chaves"),
        ])
        .with_columns([
            pl.col("_tmp_chaves").map_elements(_gerar_chave_id, return_dtype=pl.Utf8).alias("chave_id"),
            pl.col("descricao_representativa").alias("descricao_final"),
            pl.col("descricao_representativa").alias("descricao_padrao"),
            pl.col("codigo_padrao"),
            pl.col("ncm_padrao"),
            pl.col("cest_padrao"),
            pl.col("gtin_padrao"),
            pl.col("tipo_item_padrao"),
            pl.col("unid_padrao"),
            pl.col("co_sefin_agr").alias("co_sefin_padrao"),
            pl.col("co_sefin_agr_divergente").fill_null(False).alias("co_sefin_divergente"),
            pl.col("score_consistencia").fill_null(0.0).alias("score_confianca"),
            pl.lit("automatico").alias("origem_agregacao"),
            pl.lit("pendente").alias("status_revisao"),
            pl.lit(False).alias("verificado_manualmente"),
            pl.lit(None, dtype=pl.Utf8).alias("usuario_verificacao"),
            pl.lit(None, dtype=pl.Utf8).alias("data_verificacao"),
            pl.lit(None, dtype=pl.Utf8).alias("observacao_revisor"),
        ])
        .drop("_tmp_chaves")
        .select([
            "chave_id",
            "chave_produto",
            "lista_chave_produto",
            "descricao_normalizada",
            "descricao_final",
            "descricao_padrao",
            "codigo_padrao",
            "ncm_padrao",
            "cest_padrao",
            "gtin_padrao",
            "tipo_item_padrao",
            "unid_padrao",
            "co_sefin_padrao",
            "co_sefin_divergente",
            "lista_chave_item_individualizado",
            "lista_item_seq_id",
            "lista_codigos",
            "lista_descricoes_originais",
            "lista_ncm",
            "lista_cest",
            "lista_gtin",
            "lista_tipo_item",
            "lista_unids",
            "lista_co_sefin_inferido",
            "total_entradas",
            "total_saidas",
            "qtd_entradas",
            "qtd_saidas",
            "qtd_itens_origem",
            "score_confianca",
            "origem_agregacao",
            "status_revisao",
            "verificado_manualmente",
            "usuario_verificacao",
            "data_verificacao",
            "observacao_revisor",
        ])
        .sort("descricao_final")
    )

    ok1 = salvar_para_parquet(df_resultado, pasta_produtos, f"tabela_produtos_consolidados_{cnpj}.parquet")
    ok2 = salvar_para_parquet(df_resultado, pasta_produtos, f"tabela_produtos_editavel_{cnpj}.parquet")
    return ok1 and ok2


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

    sucesso = gerar_produtos_consolidados(cnpj_arg)
    sys.exit(0 if sucesso else 1)
