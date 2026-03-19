"""
Módulo para exportar DataFrame para Excel.
"""
from pathlib import Path
from rich import print as rprint


def exportar_excel(df, nome_base, diretorio_saida: Path):
    """
    Exporta um DataFrame para Excel no diretório especificado.
    
    Args:
        df: DataFrame (Pandas ou Polars)
        nome_base: Nome base do arquivo (sem extensão)
        diretorio_saida: Path object apontando para a pasta onde salvar
    """
    is_empty = False
    if hasattr(df, "height"):
        is_empty = df.height == 0
    else:
        is_empty = bool(df.empty)

    if is_empty:
        rprint("[yellow]=> Sem resultados. Arquivo Excel não gerado.[/yellow]")
        return None
    
    diretorio_saida.mkdir(parents=True, exist_ok=True)
    
    nome_arquivo = f"{nome_base}.xlsx"
    arquivo_excel = diretorio_saida / nome_arquivo
    
    # Se for Polars, usa o método nativo write_excel. Se Pandas, to_excel.
    if hasattr(df, "write_excel"):
        df.write_excel(arquivo_excel)
    else:
        df.to_excel(arquivo_excel, index=False)
        
    rprint(f"[green]   => Relatório Excel exportado:[/green] {arquivo_excel.name}")
    
    return arquivo_excel
