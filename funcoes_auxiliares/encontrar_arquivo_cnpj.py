from pathlib import Path

def encontrar_arquivo(diretorio: Path, prefixo: str, cnpj: str):
    """Busca arquivo Parquet por prefixo e CNPJ no diretório especificado, priorizando os agrupados."""
    # 1. Tentar primeiro o arquivo já AGRUPADO
    padrao_agrupado = f"{prefixo}_AGRUPADO_{cnpj}.parquet"
    arquivos_agrupados = list(diretorio.glob(padrao_agrupado))
    if arquivos_agrupados:
        return arquivos_agrupados[0]

    # 2. Tentar o padrão normal
    padrao = f"{prefixo}_{cnpj}.parquet"
    arquivos = list(diretorio.glob(padrao))
    if arquivos:
        return arquivos[0]
    
    # 3. Fallback: buscar com prefixo parcial
    for arq in diretorio.glob("*.parquet"):
        if prefixo.lower() in arq.stem.lower() and cnpj in arq.stem:
            return arq
    return None