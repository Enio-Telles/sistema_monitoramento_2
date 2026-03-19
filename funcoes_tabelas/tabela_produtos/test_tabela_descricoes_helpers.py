import pytest
from funcoes_tabelas.tabela_produtos.tabela_descricoes import calcular_moda, gerar_chave_produto

def test_calcular_moda_basico():
    assert calcular_moda(["A", "B", "A", "C"]) == "A"

def test_calcular_moda_vazio():
    assert calcular_moda([]) is None
    assert calcular_moda(None) is None

def test_calcular_moda_filtro_nulos():
    assert calcular_moda(["A", None, "", " ", "B", "B"]) == "B"

def test_calcular_moda_empate():
    # Deve retornar o primeiro em ordem alfabética: "A"
    assert calcular_moda(["B", "A", "B", "A"]) == "A"

def test_calcular_moda_lista_vazia_apos_filtro():
    assert calcular_moda([None, "", "  "]) is None

def test_gerar_chave_produto_basico():
    res = gerar_chave_produto(["1", "2", "3"])
    assert isinstance(res, str)
    assert len(res) == 32 # MD5 hash length

def test_gerar_chave_produto_consistencia():
    assert gerar_chave_produto(["1", "2", "3"]) == gerar_chave_produto(["3", "2", "1"])

def test_gerar_chave_produto_vazio():
    assert gerar_chave_produto([]) == ""
    assert gerar_chave_produto(None) == ""

def test_gerar_chave_produto_tipos():
    assert gerar_chave_produto([1, 2, 3]) == gerar_chave_produto(["1", "2", "3"])
