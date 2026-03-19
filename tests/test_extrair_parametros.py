import pytest
from funcoes_auxiliares.extrair_parametros import extrair_parametros_sql

@pytest.mark.parametrize(
    "sql, expected",
    [
        # Caso base: sem parâmetros
        ("SELECT * FROM tabela", set()),

        # Um parâmetro simples
        ("SELECT * FROM tabela WHERE id = :id_tabela", {"id_tabela"}),

        # Múltiplos parâmetros
        ("SELECT * FROM tabela WHERE id = :id AND data > :data_inicio AND status = :status", {"id", "data_inicio", "status"}),

        # Parâmetros repetidos devem aparecer apenas uma vez no conjunto
        ("SELECT * FROM tabela WHERE id = :id OR outro_id = :id", {"id"}),

        # Parâmetro colado em parênteses
        ("INSERT INTO tabela (id, nome) VALUES (:id, :nome)", {"id", "nome"}),

        # Parâmetro seguido de quebra de linha ou espaço
        ("SELECT * FROM tabela WHERE\n id = :id\n", {"id"}),

        # Sem espaços ao redor do parâmetro
        ("SELECT * FROM tabela WHERE id=:id", {"id"}),

        # Apenas um parâmetro e nada mais
        (":param", {"param"}),

        # Apenas string vazia
        ("", set()),
    ]
)
def test_extrair_parametros_sql(sql, expected):
    """
    Testa a extração de bind variables do SQL.
    Verifica se os parâmetros são corretamente identificados em diferentes contextos da query.
    """
    assert extrair_parametros_sql(sql) == expected
