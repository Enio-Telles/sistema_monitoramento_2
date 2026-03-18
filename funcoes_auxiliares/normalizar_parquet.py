import polars as pl
from typing import Optional, Union

def normalizar_colunas_parquet(df: Optional[Union[pl.DataFrame, pl.LazyFrame]]) -> Optional[Union[pl.DataFrame, pl.LazyFrame]]:
    """
    Normaliza os nomes das colunas de um DataFrame ou LazyFrame do Polars
    para letras minúsculas a fim de evitar erros de case sensitivity.
    Geralmente as colunas vindas do banco Oracle são totalmente em maiúsculas,
    o que quebra manipulações via Polars se escritas em minúsculas ou vice-versa.
    """
    if df is not None:
        if isinstance(df, pl.DataFrame) and df.is_empty():
            return df
        if isinstance(df, pl.LazyFrame):
            return df.rename({c: c.lower() for c in df.collect_schema().names()})
        else: # df is a pl.DataFrame (and not empty)
            return df.rename({c: c.lower() for c in df.columns})
    return df
