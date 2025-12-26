"""Utilitários compartilhados do projeto VIC."""

from .helpers import (
    # Funções de texto (originalmente text.py)
    normalize_ascii_upper,
    digits_only,
    # Funções de anti-join (originalmente anti_join.py)
    procv_left_minus_right,
    procv_max_menos_vic,
    procv_vic_menos_max,
    # Funções auxiliares genéricas
    primeiro_valor,
    normalizar_data_string,
    extrair_data_referencia,
    formatar_valor_string,
    extrair_telefone,
    formatar_datas_serie,
    # Funções de aging (originalmente aging.py)
    filtrar_clientes_criticos,
    # Funções de log parsing (originalmente log_parser.py)
    parse_extraction_summary,
    clean_extraction_line,
    extract_extraction_value,
)
from .filters import VicFilterApplier
from .logger import get_logger, log_section

__all__ = [
    "filtrar_clientes_criticos",
    "procv_left_minus_right",
    "procv_max_menos_vic",
    "procv_vic_menos_max",
    "normalize_ascii_upper",
    "digits_only",
    "primeiro_valor",
    "normalizar_data_string",
    "extrair_data_referencia",
    "formatar_valor_string",
    "extrair_telefone",
    "formatar_datas_serie",
    "parse_extraction_summary",
    "clean_extraction_line",
    "extract_extraction_value",
    "VicFilterApplier",
    "get_logger",
    "log_section",
]
