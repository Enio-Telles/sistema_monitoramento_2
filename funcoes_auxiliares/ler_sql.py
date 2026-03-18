from pathlib import Path

def ler_sql(arquivo):
    """
    Lê arquivo SQL com tratamento robusto de encoding.
    
    Args:
        arquivo (str or Path): Caminho para o arquivo SQL
        
    Returns:
        str: Conteúdo do arquivo SQL limpo (strip e rstrip ';')
        None: Se houver erro na leitura
    """
    if isinstance(arquivo, str):
        arquivo = Path(arquivo)
        
    # Lista de encodings para tentar
    encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1', 'cp1250']
    
    for enc in encodings:
        try:
            # Tenta ler com o encoding atual
            sql_txt = arquivo.read_text(encoding=enc)            
            
            # Limpeza básica (conforme execucao_extracao_parametros.py)
            sql_txt = sql_txt.strip().rstrip(';')
            
            # print(f"  ✅ {arquivo.name} lido com sucesso ({enc})")
            return sql_txt
            
        except UnicodeDecodeError:
            # Falha de encoding, tenta o próximo silenciosamente
            continue    
        except Exception as e:
            print(f"  ⚠️ Erro técnico ao tentar ler {arquivo.name} com {enc}: {e}")
            continue 
            
    # Se chegou aqui, não conseguiu ler com nenhum encoding
    raise Exception(f"❌ ERRO FATAL: Não foi possível ler o arquivo '{arquivo.name}' com nenhum dos encodings disponíveis.")