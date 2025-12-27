#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Script de extração de emails do Tabelionato.

Este script é um wrapper fino que delega para o extrator consolidado em:
src/core/extractor.py -> TabelionatoEmailExtractor

A lógica completa foi movida para o módulo extractor para:
- Reutilização em outros contextos (main.py, testes)
- Separação de responsabilidades
- Redução de código duplicado
"""

import os
import sys
from pathlib import Path

# Configuração de paths
BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))

from src.core.extractor import TabelionatoEmailExtractor


def main() -> None:
    """Função principal - delega para o extrator consolidado."""
    debug_mode = ('--debug' in sys.argv) or (os.getenv('TABELIONATO_DEBUG', '0') == '1')

    extractor = TabelionatoEmailExtractor(
        base_path=BASE,
        dias=7,
        debug=debug_mode
    )
    extractor.extrair()


if __name__ == "__main__":
    main()
