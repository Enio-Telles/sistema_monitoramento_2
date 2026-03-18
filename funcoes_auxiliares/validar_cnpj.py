
import re

def validar_cnpj(cnpj: str) -> bool:
    """
    Valida se um CNPJ é válido (dígitos verificadores e formato).
    
    Args:
        cnpj: String contendo o CNPJ (pode incluir .,-/)
        
    Returns:
        True se válido, False caso contrário.
    """
    # Remover caracteres não numéricos
    cnpj = re.sub(r'[^0-9]', '', cnpj)

    # Verificar tamanho
    if len(cnpj) != 14:
        return False

    # Verificar se todos os dígitos são iguais (ex: 11111111111111)
    if len(set(cnpj)) == 1:
        return False

    # Validação do primeiro dígito verificador
    tamanho = 12
    numeros = cnpj[:tamanho]
    digitos = cnpj[tamanho:]
    soma = 0
    pos = tamanho - 7
    for i in range(tamanho, 0, -1):
        soma += int(numeros[tamanho - i]) * pos
        pos -= 1
        if pos < 2:
            pos = 9
    
    resultado = soma % 11
    if resultado < 2:
        digito_1 = 0
    else:
        digito_1 = 11 - resultado

    if digito_1 != int(digitos[0]):
        return False

    # Validação do segundo dígito verificador
    tamanho = 13
    numeros = cnpj[:tamanho]
    soma = 0
    pos = tamanho - 7
    for i in range(tamanho, 0, -1):
        soma += int(numeros[tamanho - i]) * pos
        pos -= 1
        if pos < 2:
            pos = 9

    resultado = soma % 11
    if resultado < 2:
        digito_2 = 0
    else:
        digito_2 = 11 - resultado

    if digito_2 != int(digitos[1]):
        return False

    return True
