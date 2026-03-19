import unittest
from funcoes_auxiliares.validar_cnpj import validar_cnpj

class TestValidarCNPJ(unittest.TestCase):
    def test_valid_cnpj_with_format(self):
        self.assertTrue(validar_cnpj('11.222.333/0001-81'))
        self.assertTrue(validar_cnpj('00.000.000/0001-91'))

    def test_valid_cnpj_without_format(self):
        self.assertTrue(validar_cnpj('11222333000181'))
        self.assertTrue(validar_cnpj('00000000000191'))

    def test_invalid_length(self):
        self.assertFalse(validar_cnpj('1122233300018'))
        self.assertFalse(validar_cnpj('112223330001811'))

    def test_all_digits_same(self):
        self.assertFalse(validar_cnpj('11111111111111'))
        self.assertFalse(validar_cnpj('00000000000000'))

    def test_invalid_check_digits(self):
        self.assertFalse(validar_cnpj('11.222.333/0001-82'))
        self.assertFalse(validar_cnpj('00.000.000/0001-92'))

    def test_empty_or_non_numeric(self):
        self.assertFalse(validar_cnpj(''))
        self.assertFalse(validar_cnpj('abc'))
        self.assertFalse(validar_cnpj('./-'))

if __name__ == '__main__':
    unittest.main()
