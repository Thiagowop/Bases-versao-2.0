"""Funções auxiliares reutilizáveis para processadores Tabelionato.

Consolida funções de:
- console.py (format_int, format_duration, print_section, etc.)
- formatting.py (formatar_moeda_serie)
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Iterable, List, Optional, Sequence

import pandas as pd


# =============================================================================
# FUNÇÕES DE FORMATAÇÃO DE CONSOLE (originalmente em console.py)
# =============================================================================

BORDER = "=" * 60


def format_int(value: int | None) -> str:
    """Formata inteiros com separador de milhar."""
    if value is None:
        return "-"
    return f"{value:,}"


def format_percent(value: float, *, precision: int = 1) -> str:
    """Formata percentuais com casas decimais controladas."""
    return f"{value:.{precision}f}%"


def format_duration(seconds: float, *, precision: int = 1) -> str:
    """Formata durações em segundos."""
    return f"{seconds:.{precision}f}s"


def suppress_console_info(logger: logging.Logger, level: int = logging.WARNING) -> None:
    """Eleva o nível dos handlers de console para reduzir ruído."""
    for handler in logger.handlers:
        if isinstance(handler, logging.StreamHandler):
            handler.setLevel(level)


def print_section(
    title: str,
    lines: Sequence[str],
    *,
    leading_break: bool = True,
    trailing_break: bool = True,
) -> None:
    """Imprime um bloco padronizado com borda e corpo."""
    if leading_break:
        print()

    print(BORDER)
    print(title)
    print(BORDER)
    print()

    for line in lines:
        if line:
            print(line)
        else:
            print()

    if trailing_break:
        print()


def print_list(title: str, items: Iterable[str]) -> None:
    """Imprime uma lista simples precedida de um título."""
    print(title)
    for item in items:
        print(f"- {item}")


# =============================================================================
# FUNÇÕES DE FORMATAÇÃO DE DADOS (originalmente em formatting.py)
# =============================================================================

def formatar_moeda_serie(
    serie: pd.Series,
    *,
    decimal_separator: str = ",",
) -> pd.Series:
    """Normaliza valores monetários e retorna strings formatadas com duas casas."""
    texto = (
        serie.astype(str)
        .str.replace("R$", "", regex=False)
        .str.replace(" ", "", regex=False)
        .str.strip()
    )

    possui_virgula = texto.str.contains(",", na=False)
    texto_normalizado = texto.copy()
    texto_normalizado[possui_virgula] = (
        texto_normalizado[possui_virgula]
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    texto_normalizado[~possui_virgula] = texto_normalizado[~possui_virgula].str.replace(",", ".", regex=False)

    valores = pd.to_numeric(texto_normalizado, errors="coerce")
    return valores.map(lambda valor: "" if pd.isna(valor) else ("%.2f" % valor).replace(".", decimal_separator))


# =============================================================================
# FUNÇÕES AUXILIARES GENÉRICAS
# =============================================================================

def primeiro_valor(series: Optional[pd.Series]) -> Optional[Any]:
    """Retorna o primeiro valor não-nulo de uma Series."""
    if series is None or series.empty:
        return None
    
    valores_validos = series.dropna()
    if valores_validos.empty:
        return None
    
    return valores_validos.iloc[0]


def normalizar_data_string(valor: Any, formato_saida: str = "%d/%m/%Y") -> Optional[str]:
    """Normaliza valores de data para string no formato especificado."""
    if pd.isna(valor) or valor == "":
        return None
    
    if isinstance(valor, (pd.Timestamp, datetime)):
        return valor.strftime(formato_saida)
    
    try:
        dt = pd.to_datetime(valor, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.strftime(formato_saida)
    except Exception:
        return None


__all__ = [
    # Funções de console
    "format_int",
    "format_percent",
    "format_duration",
    "suppress_console_info",
    "print_section",
    "print_list",
    "BORDER",
    # Funções de formatação
    "formatar_moeda_serie",
    # Funções auxiliares
    "primeiro_valor",
    "normalizar_data_string",
    # Funções de normalização de texto (consolidadas de extrair_email.py)
    "normalize_text",
    "normalize_data_tabelionato",
    "normalize_cep",
    "normalize_currency",
    "normalize_bool",
    "normalize_ascii_lower",
]


# =============================================================================
# FUNÇÕES DE NORMALIZAÇÃO DE TEXTO (consolidadas de extrair_email.py)
# =============================================================================

import re
import unicodedata

_SPACE_RE = re.compile(r'\s+')
_NON_DIGIT_RE = re.compile(r'\D')
_BOOL_MAP = {
    'false': 'False',
    'falso': 'False',
    'true': 'True',
    'verdadeiro': 'True'
}


def normalize_ascii_lower(text: str | None) -> str:
    """Normaliza texto removendo acentos e espacos extras para comparacoes."""
    if not text:
        return ""
    normalized = unicodedata.normalize("NFKD", text)
    without_accents = ''.join(ch for ch in normalized if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", without_accents).strip().lower()


def normalize_text(value: str) -> str:
    """Normaliza texto removendo espaços extras."""
    return _SPACE_RE.sub(' ', value.strip()) if value else ''


def normalize_data_tabelionato(value: str) -> str:
    """Normaliza datas para formato DD/MM/YYYY HH:MM:SS."""
    value = normalize_text(value)
    if not value:
        return value
    if re.fullmatch(r'\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}', value):
        return value
    if re.fullmatch(r'\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}', value):
        return f"{value}:00"
    if re.fullmatch(r'\d{2}/\d{2}/\d{4}', value):
        return f"{value} 00:00:00"
    return value


def normalize_cep(value: str) -> str:
    """Normaliza CEP mantendo apenas 8 dígitos."""
    digits = _NON_DIGIT_RE.sub('', value or '')
    if len(digits) == 8:
        return digits
    return normalize_text(value)


def normalize_currency(value: str) -> str:
    """Normaliza valores monetários removendo R$ e espaços."""
    if not value:
        return ''
    cleaned = value.upper().replace('R$', '').strip()
    cleaned = cleaned.replace(' ', '')
    return cleaned if cleaned else ''


def normalize_bool(value: str) -> str:
    """Normaliza valores booleanos (True/False/Verdadeiro/Falso)."""
    if not value:
        return ''
    return _BOOL_MAP.get(value.strip().lower(), value.strip())
