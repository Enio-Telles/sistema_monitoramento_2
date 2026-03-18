"""
Módulo de conexão Oracle.
Usa credenciais do arquivo .env
"""
from dotenv import load_dotenv
import oracledb
import socket
from rich import print as rprint
import os

from pathlib import Path

# Force load of .env file from project root
# current file is in funcoes_auxiliares/, so project root is one level up
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path, encoding='latin-1', override=True)

HOST = os.getenv("ORACLE_HOST", 'exa01-scan.sefin.ro.gov.br').strip()
PORTA_STR = os.getenv("ORACLE_PORT", '1521').strip()
PORTA = int(PORTA_STR)
SERVICO = os.getenv("ORACLE_SERVICE", 'sefindw').strip()

def conectar(cpf_usuario=None, senha=None):
    # Validar variáveis globais
    if not HOST or not PORTA or not SERVICO:
        rprint("[yellow]Aviso:[/yellow] Variáveis de conexão (HOST, PORTA, SERVICO) não definidas corretamente.")
    
    if cpf_usuario is None:
        cpf_usuario = os.getenv("DB_USER")
    if senha is None:
        senha = os.getenv("DB_PASSWORD")
    
    if cpf_usuario:
        cpf_usuario = cpf_usuario.strip()
    if senha:
        senha = senha.strip()
    
    if not cpf_usuario or not senha:
        rprint("[red]Erro:[/red] Credenciais não encontradas no .env")
        rprint("Verifique se o arquivo .env existe e tem DB_USER e DB_PASSWORD")
        return None
    
    try:
        dsn = oracledb.makedsn(HOST, PORTA, service_name=SERVICO)
        rprint(f"[cyan]DEBUG: Tentando conectar a HOST='{HOST}' PORTA={PORTA} SERVICO='{SERVICO}'[/cyan]")
        rprint(f"[cyan]DEBUG: DSN='{dsn}'[/cyan]")
        try:
           ip = socket.gethostbyname(HOST)
           rprint(f"[cyan]DEBUG: Host resolvido para IP: {ip}[/cyan]")
        except Exception as e_dns:
           rprint(f"[red]DEBUG: Falha na resolução DNS de '{HOST}': {e_dns}[/red]")
        
        conexao = oracledb.connect(user=cpf_usuario, password=senha, dsn=dsn)
        
        # Injetar formatação de sessão globalmente para prevenir ORA-01722
        try:
            with conexao.cursor() as cursor:
                cursor.execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'")
        except Exception as e_nls:
            rprint(f"[yellow]Aviso: Falha ao configurar NLS_NUMERIC_CHARACTERS: {e_nls}[/yellow]")

        rprint("[green]=> Conectado ao Oracle (NLS configurado)[/green]")
        return conexao
    except Exception as e:
        rprint(f"[red]Erro de conexão:[/red] {e}")
        return None
      
if __name__ == "__main__":    
    conexao = conectar()
    if conexao:
        rprint("[green]Conexão bem sucedida[/green]")
        conexao.close()
        rprint("[green]Conexão fechada[/green]")
    else:
        rprint("[red]Falha na conexão[/red]")
    
    
    
    