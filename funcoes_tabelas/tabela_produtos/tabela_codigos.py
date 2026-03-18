"""
tabela_codigos.py

Script para ler a tabela normalizada de produtos e identificar códigos (cod_normalizado)
que possuam mais de uma descrição (descricao) associada.
Gera uma nova tabela contendo as listas de chaves e descrições, além de um código desagregado para cada item.
"""

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


def tabela_codigos_mais_descricoes(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    import re
    cnpj = re.sub(r"[^0-9]", "", cnpj)

    if pasta_cnpj is None:
        pasta_cnpj = FUNCOES_DIR / "CNPJ" / cnpj

    pasta_produtos = pasta_cnpj / "analises" / "produtos"
    arq_entrada = pasta_produtos / f"tab_itens_caract_normalizada_{cnpj}.parquet"

    if not arq_entrada.exists():
        rprint(f"[red]Erro: Arquivo normalizado não encontrado: {arq_entrada}[/red]")
        return False

    rprint(f"\n[bold cyan]Gerando tabela_codigos_mais_descricoes para CNPJ: {cnpj}[/bold cyan]")

    # Lê o parquet e pega as colunas necessárias
    df = pl.read_parquet(arq_entrada, columns=["cod_normalizado", "chave_item_individualizado", "descricao"])

    # Remove nulos/vazios no código
    df = df.filter(
        pl.col("cod_normalizado").is_not_null() & (pl.col("cod_normalizado") != "")
    )

    # Verifica códigos que têm mais de 1 descrição MUDADA/DIFERENTE
    df_filtrado = (
        df.filter(pl.col("descricao").n_unique().over("cod_normalizado") > 1)
          .sort(["cod_normalizado", "descricao"])
    )

    if df_filtrado.is_empty():
        rprint("[yellow]Nenhum cod_normalizado com múltiplas descrições encontrado.[/yellow]")
        # Mesmo assim, podemos salvar uma tabela vazia com o schema correto
        pass

    # Cria a coluna com sufixo sequencial
    # cum_count conta elementos não nulos cumulativamente, resultando em 1, 2, 3...
    col_seq = pl.col("chave_item_individualizado").cum_count().over("cod_normalizado").cast(pl.String)
    
    df_com_sufixo = df_filtrado.with_columns(
        (pl.col("cod_normalizado") + "__" + col_seq).alias("cods_desagregados")
    )

    # Agrupar para criar as listas finais
    df_resultado = (
        df_com_sufixo.group_by("cod_normalizado")
        .agg([
            pl.col("chave_item_individualizado").alias("lista_chave_item_individualizado"),
            pl.col("descricao").alias("lista_descricao"),
            pl.col("cods_desagregados"),
            pl.col("descricao").n_unique().alias("qtd_descr")
        ])
        .sort("cod_normalizado")
    )

    rprint(f"[green]Total de cod_normalizado com >1 descrição: {len(df_resultado)}[/green]")

    nome_saida = f"tabela_codigos_mais_descricoes_{cnpj}.parquet"
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

    sucesso = tabela_codigos_mais_descricoes(cnpj_arg)
    sys.exit(0 if sucesso else 1)
