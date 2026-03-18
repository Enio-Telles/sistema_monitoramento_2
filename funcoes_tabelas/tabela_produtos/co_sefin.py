"""
co_sefin.py

Script para inferir o código co_sefin_inferido com base no NCM e CEST
utilizando tabelas de referência sitafe.
"""

import sys
from pathlib import Path

import polars as pl
from rich import print as rprint

FUNCOES_DIR = Path(r"c:\funcoes")
AUXILIARES_DIR = FUNCOES_DIR / "funcoes_auxiliares"
REFS_DIR = FUNCOES_DIR / "referencias" / "CO_SEFIN"

if str(AUXILIARES_DIR) not in sys.path:
    sys.path.insert(0, str(AUXILIARES_DIR))

try:
    from salvar_para_parquet import salvar_para_parquet
    from validar_cnpj import validar_cnpj
except ImportError as e:
    rprint(f"[red]Erro ao importar módulos auxiliares:[/red] {e}")
    sys.exit(1)


def co_sefin(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    import re
    cnpj = re.sub(r"[^0-9]", "", cnpj)

    if pasta_cnpj is None:
        pasta_cnpj = FUNCOES_DIR / "CNPJ" / cnpj

    pasta_produtos = pasta_cnpj / "analises" / "produtos"
    
    arquivos_alvo = [
        pasta_produtos / f"tabela_itens_caracteristicas_{cnpj}.parquet",
        pasta_produtos / f"tab_itens_caract_normalizada_{cnpj}.parquet"
    ]

    rprint(f"\n[bold cyan]Inferindo co_sefin_inferido para CNPJ: {cnpj}[/bold cyan]")

    # 1. Carregar tabelas de referência
    ref_cest_ncm_path = REFS_DIR / "sitafe_cest_ncm.parquet"
    ref_cest_path = REFS_DIR / "sitafe_cest.parquet"
    ref_ncm_path = REFS_DIR / "sitafe_ncm.parquet"

    for p in [ref_cest_ncm_path, ref_cest_path, ref_ncm_path]:
        if not p.exists():
            rprint(f"[red]ERRO: Tabela de referência não encontrada: {p}[/red]")
            return False

    def _limpar_str(col):
        return pl.col(col).cast(pl.String).str.replace_all(r"\.", "").str.strip_chars()

    # Prepara referencias (lazy para joins eficientes)
    # sitafe_cest_ncm: it_nu_cest, it_nu_ncm, it_co_sefin
    ref_cn = (
        pl.scan_parquet(ref_cest_ncm_path)
        .select(["it_nu_cest", "it_nu_ncm", "it_co_sefin"])
        .with_columns([
            _limpar_str("it_nu_cest").alias("ref_cest"),
            _limpar_str("it_nu_ncm").alias("ref_ncm")
        ])
        .drop(["it_nu_cest", "it_nu_ncm"])
    )

    # sitafe_cest: cest, co-sefin
    ref_c = (
        pl.scan_parquet(ref_cest_path)
        .select(["cest", "co-sefin"])
        .with_columns([
            _limpar_str("cest").alias("ref_cest")
        ])
        .drop("cest")
        .rename({"co-sefin": "co_sefin_cest"})
    )

    # sitafe_ncm: ncm, co-sefin
    ref_n = (
        pl.scan_parquet(ref_ncm_path)
        .select(["ncm", "co-sefin"])
        .with_columns([
            _limpar_str("ncm").alias("ref_ncm")
        ])
        .drop("ncm")
        .rename({"co-sefin": "co_sefin_ncm"})
    )

    sucesso_total = True

    encontrado_qualquer = False
    for arq in arquivos_alvo:
        if not arq.exists():
            rprint(f"[yellow]Aviso: Arquivo não encontrado: {arq.resolve()}[/yellow]")
            continue

        encontrado_qualquer = True
        rprint(f"[cyan]Processando {arq.name}...[/cyan]")
        
        df_alvo = pl.scan_parquet(arq)
        
        # Garante que NCM e CEST estejam limpos para o join
        # Na tabela normalizada eles já devem estar limpos de pontos conforme regra anterior
        # mas na original podem não estar. Vamos limpar aqui para o join sem alterar a coluna original.
        df_join = df_alvo.with_columns([
            _limpar_str("ncm").alias("_ncm_join"),
            _limpar_str("cest").alias("_cest_join")
        ])

        # Sequencia de Joins por prioridade
        # 1. CEST + NCM
        df_join = df_join.join(
            ref_cn, 
            left_on=["_cest_join", "_ncm_join"], 
            right_on=["ref_cest", "ref_ncm"], 
            how="left"
        )
        
        # 2. CEST
        df_join = df_join.join(
            ref_c,
            left_on="_cest_join",
            right_on="ref_cest",
            how="left"
        )
        
        # 3. NCM
        df_join = df_join.join(
            ref_n,
            left_on="_ncm_join",
            right_on="ref_ncm",
            how="left"
        )

        # Coalesce para pegar o primeiro valor não nulo na ordem de prioridade
        df_final = df_join.with_columns(
            pl.coalesce(["it_co_sefin", "co_sefin_cest", "co_sefin_ncm"]).alias("co_sefin_inferido")
        ).drop(["_ncm_join", "_cest_join", "it_co_sefin", "co_sefin_cest", "co_sefin_ncm"])

        # Coleta e salva
        df_result = df_final.collect()
        
        # Salva o arquivo atualizado
        # Usamos salvar_para_parquet que lida com a criação da pasta e log
        ok = salvar_para_parquet(df_result, arq.parent, arq.name)
        if not ok:
            sucesso_total = False

    return sucesso_total if encontrado_qualquer else False


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

    sucesso = co_sefin(cnpj_arg)
    sys.exit(0 if sucesso else 1)
