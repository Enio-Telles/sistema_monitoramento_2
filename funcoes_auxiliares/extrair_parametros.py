"""
Módulo auxiliar para identificar bind variables em comandos SQL.
"""
import re

def extrair_parametros_sql(sql: str) -> set[str]:
    """
    Identifica variáveis de ligação (bind variables) no formato :nome_variavel dentro de uma string SQL.
    
    Args:
        sql (str): O conteúdo do SQL.
        
    Returns:
        set[str]: Um conjunto contendo os nomes das variáveis encontradas (sem o dois-pontos).
    """
    # Expressão regular para encontrar :variavel
    # \b garante que não pegue dois pontos no meio de strings se não for variável (embora SQL tenha suas especificidades)
    # A regex r":(\w+)" captura palavras que começam com :
    binds = re.findall(r":(\w+)", sql)
    return set(binds)
