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
]
