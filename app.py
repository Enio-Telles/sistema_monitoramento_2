"""
Lançador principal do Fiscal Parquet Analyzer.

Executa a interface gráfica a partir da raiz do projeto c:\\funcoes,
configurando o sys.path para encontrar o pacote fiscal_app
dentro de sistema_monitoramento.
"""
from __future__ import annotations

import sys
from pathlib import Path

# Diretório onde reside o pacote fiscal_app
SISTEMA_MONITORAMENTO = Path(
    r"C:\Users\03002693901\OneDrive - SECRETARIA DE ESTADO DE FINANCAS"
    r"\Desenvolvimento\sistema_monitoramento"
)

if str(SISTEMA_MONITORAMENTO) not in sys.path:
    sys.path.insert(0, str(SISTEMA_MONITORAMENTO))

from PySide6.QtWidgets import QApplication
from fiscal_app.ui.main_window import MainWindow
from fiscal_app.services.aggregation_service import ServicoAgregacao


def main() -> int:
    app = QApplication(sys.argv)
    app.setApplicationName("Fiscal Parquet Analyzer")
    window = MainWindow()
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
