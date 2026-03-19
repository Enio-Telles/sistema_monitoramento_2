from pathlib import Path

def encontrar_arquivo(diretorio: Path, prefixo: str, cnpj: str):
    """Busca arquivo Parquet por prefixo e CNPJ no diretório especificado, priorizando os agrupados."""
    # 1. Tentar primeiro o arquivo já AGRUPADO
    arquivo_agrupado = diretorio / f"{prefixo}_AGRUPADO_{cnpj}.parquet"
    if arquivo_agrupado.is_file():
        return arquivo_agrupado

    # 2. Tentar o padrão normal
    arquivo_normal = diretorio / f"{prefixo}_{cnpj}.parquet"
    if arquivo_normal.is_file():
        return arquivo_normal
    
    # 3. Fallback: buscar com prefixo parcial
    prefixo_lower = prefixo.lower()
    for arq in diretorio.glob(f"*{cnpj}*.parquet"):
        if prefixo_lower in arq.stem.lower():
            return arq
    return None
