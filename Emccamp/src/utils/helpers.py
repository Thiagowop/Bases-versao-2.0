"""Funções auxiliares reutilizáveis para processadores.

Consolida funções de:
- anti_join.py (procv_left_minus_right, procv_max_menos_emccamp, procv_emccamp_menos_max)
- text.py (normalize_ascii_upper, digits_only)
- helpers genéricos (primeiro_valor, normalizar_data_string, etc.)
"""

from __future__ import annotations

import unicodedata
from datetime import datetime
from typing import Any, Iterable, List, Optional

import pandas as pd


# =============================================================================
# FUNÇÕES DE TEXTO (originalmente em text.py)
# =============================================================================

def normalize_ascii_upper(serie: pd.Series) -> pd.Series:
    """Retorna série normalizada removendo acentos, strip e maiúsculas."""

    def _norm(txt: str) -> str:
        chars = unicodedata.normalize("NFKD", txt)
        chars = "".join(ch for ch in chars if not unicodedata.combining(ch))
        return chars.upper().strip()

    return serie.astype(str).map(_norm)


def digits_only(serie: pd.Series) -> pd.Series:
    """Remove todos os caracteres não numéricos de uma série."""
    return serie.astype(str).str.replace(r"\D", "", regex=True)


# =============================================================================
# FUNÇÕES DE ANTI-JOIN (originalmente em anti_join.py)
# =============================================================================

def _normalize_series(values: pd.Series) -> pd.Series:
    """Normaliza série para comparação eficiente (string strip)."""
    return values.astype(str).str.strip()


def procv_left_minus_right(
    df_left: pd.DataFrame,
    df_right: pd.DataFrame,
    col_left: str,
    col_right: str,
) -> pd.DataFrame:
    """Retorna linhas de df_left cujas chaves não estão em df_right."""

    if col_left not in df_left.columns:
        raise ValueError(f"Coluna obrigatória ausente no LEFT: {col_left}")
    if col_right not in df_right.columns:
        raise ValueError(f"Coluna obrigatória ausente no RIGHT: {col_right}")

    right_keys: Iterable[str] = set(_normalize_series(df_right[col_right]).dropna())
    mask = ~_normalize_series(df_left[col_left]).isin(right_keys)
    return df_left.loc[mask].copy()


def procv_max_menos_emccamp(
    df_max: pd.DataFrame,
    df_emccamp: pd.DataFrame,
    col_max: str = "PARCELA",
    col_emccamp: str = "CHAVE",
) -> pd.DataFrame:
    """Retorna registros MAX que NÃO estão em EMCCAMP (MAX - EMCCAMP)."""
    return procv_left_minus_right(df_max, df_emccamp, col_max, col_emccamp)


def procv_emccamp_menos_max(
    df_emccamp: pd.DataFrame,
    df_max: pd.DataFrame,
    col_emccamp: str = "CHAVE",
    col_max: str = "PARCELA",
) -> pd.DataFrame:
    """Retorna registros EMCCAMP que NÃO estão em MAX (EMCCAMP - MAX)."""
    return procv_left_minus_right(df_emccamp, df_max, col_emccamp, col_max)


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


def extrair_data_referencia(
    df: pd.DataFrame,
    colunas_candidatas: Optional[List[str]] = None,
    formato_saida: str = "%d/%m/%Y"
) -> str:
    """Extrai data de referência de um DataFrame."""
    if colunas_candidatas is None:
        colunas_candidatas = [
            "DATA_BASE",
            "DATA BASE",
            "DATA_EXTRACAO_BASE",
            "DATA EXTRACAO BASE",
            "DATA_EXTRACAO",
            "DATA EXTRACAO",
            "DATA_REFERENCIA",
        ]
    
    for coluna in colunas_candidatas:
        if coluna in df.columns:
            valor = primeiro_valor(df[coluna])
            normalizado = normalizar_data_string(valor, formato_saida)
            if normalizado:
                return normalizado
    
    return datetime.now().strftime(formato_saida)


__all__ = [
    # Funções de texto
    "normalize_ascii_upper",
    "digits_only",
    # Funções de anti-join
    "procv_left_minus_right",
    "procv_max_menos_emccamp",
    "procv_emccamp_menos_max",
    # Funções auxiliares
    "primeiro_valor",
    "normalizar_data_string",
    "extrair_data_referencia",
]
