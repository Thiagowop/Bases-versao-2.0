"""Módulo core - Classes base e extratores compartilhados."""

from .base_processor import BaseProcessor
from .extractor import (
    BaseExtractor,
    EmccampExtractor,
    MaxDBExtractor,
    JudicialDBExtractor,
    BaixaExtractor,
    DoublecheckExtractor,
    extrair_todas_bases,
)

__all__ = [
    'BaseProcessor',
    'BaseExtractor',
    'EmccampExtractor',
    'MaxDBExtractor',
    'JudicialDBExtractor',
    'BaixaExtractor',
    'DoublecheckExtractor',
    'extrair_todas_bases',
]

