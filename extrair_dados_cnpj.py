import sys
import os
import re
import polars as pl
from pathlib import Path
from rich import print as rprint

FUNCOES_DIR = Path(r"c:\funcoes")
AUXILIARES_DIR = FUNCOES_DIR / "funcoes_auxiliares"

if str(AUXILIARES_DIR) not in sys.path:
    sys.path.insert(0, str(AUXILIARES_DIR))

try:
    from conectar_oracle import conectar
    from ler_sql import ler_sql
    from salvar_para_parquet import salvar_para_parquet
    from validar_cnpj import validar_cnpj
except ImportError as e:
    rprint(f"[red]Erro ao importar módulos auxiliares:[/red] {e}")
    sys.exit(1)

def extrair_dados(cnpj_input, data_limite_input=None):
    if not validar_cnpj(cnpj_input):
        rprint(f"[red]Erro:[/red] CNPJ '{cnpj_input}' inválido!")
        return False
        
    cnpj_limpo = re.sub(r'[^0-9]', '', cnpj_input)
    
    # Formatar msg
    msg_inicio = f"[bold green]Iniciando extração para o CNPJ: {cnpj_limpo}[/bold green]"
    if data_limite_input:
        msg_inicio += f" [cyan](Data Limite: {data_limite_input})[/cyan]"
    rprint(msg_inicio)
    
    pasta_saida = FUNCOES_DIR / "CNPJ" / cnpj_limpo
    pasta_saida.mkdir(parents=True, exist_ok=True)
    
    rprint("[yellow]Estabelecendo conexão com o banco Oracle...[/yellow]")
    conexao = conectar()
    if not conexao:
        rprint("[red]Falha na conexão com o banco de dados. Encerrando.[/red]")
        return False
        
    consultas_dir = FUNCOES_DIR / "consultas_fonte"
    arquivos_sql = list(consultas_dir.rglob("*.sql"))
    
    if not arquivos_sql:
        rprint("[yellow]Nenhum arquivo .sql encontrado na pasta consultas_fonte.[/yellow]")
        conexao.close()
        return False
        
    rprint(f"[cyan]Encontrados {len(arquivos_sql)} arquivos de consulta para execução.[/cyan]")
    
    sucesso_geral = True
    
    try:
        with conexao.cursor() as cursor:
            # Oracle client fetch optimization
            cursor.arraysize = 1000
            
            for arq_sql in arquivos_sql:
                rprint(f"\n[bold cyan]Processando: {arq_sql.relative_to(consultas_dir)}[/bold cyan]")
                
                try:
                    sql_txt = ler_sql(arq_sql)
                    if not sql_txt:
                        rprint(f"[yellow]Arquivo {arq_sql.name} vazio ou com erro de leitura.[/yellow]")
                        continue
                    
                    cursor.prepare(sql_txt)
                    nomes_binds = cursor.bindnames()
                    
                    binds = {}
                    tem_bind_cnpj = False
                    for b in nomes_binds:
                        b_upper = b.upper()
                        if b_upper == "CNPJ":
                            binds[b] = cnpj_limpo
                            tem_bind_cnpj = True
                        elif b_upper == "DATA_LIMITE_PROCESSAMENTO":
                            binds[b] = data_limite_input if data_limite_input else None
                    
                    # Executa consulta
                    if not tem_bind_cnpj:
                        if nomes_binds:
                            rprint(f"[yellow]⚠️ Consulta possui os binds ({', '.join(nomes_binds)}) mas não o :CNPJ. Pulando para evitar extração imensa.[/yellow]")
                        else:
                            rprint("[yellow]⚠️ Consulta não possui nenhuma variável de bind. Pulando para evitar extração imensa da base.[/yellow]")
                        continue
                        
                    cursor.execute(None, binds)
                    
                    colunas = [col[0] for col in cursor.description]
                    dados = cursor.fetchall()
                    
                    if not dados:
                        rprint(f"[yellow]  Zero linhas retornadas. Pulando gravação.[/yellow]")
                        continue
                        
                    df = pl.DataFrame(dados, schema=colunas, orient="row")
                    rprint(f"[green]  {len(df)} linhas lidas com sucesso.[/green]")
                    
                    # Nome do arquivo no formato nomedaconsulta_<cnpj>.parquet (mantendo subpastas se houver)
                    caminho_relativo = arq_sql.relative_to(consultas_dir)
                    nome_arquivo = f"{arq_sql.stem}_{cnpj_limpo}.parquet"
                    arquivo_saida = pasta_saida / caminho_relativo.parent / nome_arquivo
                    
                    salvar_para_parquet(df, arquivo_saida)
                    
                except Exception as e_proc:
                    rprint(f"[red]  ❌ Erro processando {arq_sql.name}: {e_proc}[/red]")
                    sucesso_geral = False
                    
    finally:
        conexao.close()
        rprint("\n[bold green]Conexão encerrada.[/bold green]")
        
    return sucesso_geral

def main():
    data_limite_arg = None
    if len(sys.argv) > 1:
        cnpj_arg = sys.argv[1]
        if len(sys.argv) > 2:
            data_limite_arg = sys.argv[2]
    else:
        try:
            cnpj_arg = input("Informe o CNPJ para extração: ").strip()
            if cnpj_arg:
                data_limite_arg = input("Data Limite Processamento (DD/MM/YYYY) [opcional, Enter para pular]: ").strip()
                if not data_limite_arg:
                    data_limite_arg = None
        except KeyboardInterrupt:
            rprint("\n[yellow]Operação cancelada pelo usuário.[/yellow]")
            sys.exit(0)
        except EOFError:
            sys.exit(0)
            
    if not cnpj_arg:
        rprint("[red]Erro: CNPJ não fornecido.[/red]")
        sys.exit(1)
        
    extrair_dados(cnpj_arg, data_limite_arg)

if __name__ == "__main__":
    main()
