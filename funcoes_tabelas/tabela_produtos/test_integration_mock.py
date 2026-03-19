import sys
from unittest.mock import MagicMock, patch
from pathlib import Path

# Mock modules that are difficult to install or rely on specific environment
sys.modules['polars'] = MagicMock()
sys.modules['rich'] = MagicMock()
sys.modules['salvar_para_parquet'] = MagicMock()
sys.modules['validar_cnpj'] = MagicMock()

import polars as pl
from funcoes_tabelas.tabela_produtos.tabela_descricoes import gerar_tabela_descricoes

def test_gerar_tabela_descricoes_integration():
    mock_df = MagicMock()
    mock_df.columns = ["descricao", "chave_item_individualizado", "cod_normalizado", "total_entradas", "total_saidas"]

    # Mocking the chain of calls in Polars
    mock_df.group_by.return_value.agg.return_value.with_columns.return_value.sort.return_value = mock_df
    mock_df.__len__.return_value = 10

    with patch('polars.read_parquet', return_value=mock_df):
        with patch('pathlib.Path.exists', return_value=True):
            with patch('funcoes_tabelas.tabela_produtos.tabela_descricoes.salvar_para_parquet', return_value=True):
                result = gerar_tabela_descricoes("12345678000199")
                assert result is True

if __name__ == "__main__":
    test_gerar_tabela_descricoes_integration()
    print("Integration test passed!")
