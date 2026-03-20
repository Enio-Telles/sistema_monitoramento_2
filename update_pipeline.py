import re

file_path = "fiscal_app/services/pipeline_funcoes_service.py"
with open(file_path, "r") as f:
    content = f.read()

# Substituir a definição de TABELAS_DISPONIVEIS
new_tabelas = """TABELAS_DISPONIVEIS = [
    {
        "id": "produtos_unidades",
        "nome": "Movimentações por Unidade",
        "modulo": "produtos_unidades",
        "funcao": "gerar_produtos_unidades"
    },
    {
        "id": "produtos",
        "nome": "Produtos Normalizados",
        "modulo": "produtos",
        "funcao": "gerar_tabela_produtos"
    },
    {
        "id": "produtos_agrupados",
        "nome": "Agrupamento Final",
        "modulo": "produtos_agrupados",
        "funcao": "gerar_produtos_agrupados"
    },
    {
        "id": "fatores_conversao",
        "nome": "Fatores de Conversão",
        "modulo": "fatores_conversao",
        "funcao": "gerar_fatores_conversao"
    }
]"""

# Replace existing TABELAS_DISPONIVEIS logic
content = re.sub(r"TABELAS_DISPONIVEIS\s*=\s*\[(.*?)\]", new_tabelas, content, flags=re.DOTALL)

# Update ordem
content = re.sub(
    r"ordem\s*=\s*\[.*?\]",
    'ordem = ["produtos_unidades", "produtos", "produtos_agrupados", "fatores_conversao"]',
    content
)

# Remove the explicit _importar_funcao_tabela handling that was hardcoded for the old file
content = re.sub(
    r"if nome_funcao == \"gerar_tabela_codigos\":\s+nome_funcao = \"tabela_codigos_mais_descricoes\"",
    "",
    content
)

with open(file_path, "w") as f:
    f.write(content)
