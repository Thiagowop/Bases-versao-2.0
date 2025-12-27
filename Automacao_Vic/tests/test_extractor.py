#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Testes para o módulo extractor."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import pandas as pd

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.extractor import (
    VicEmailExtractor,
    MaxDBExtractor,
    JudicialDBExtractor,
    _normalize_text,
    _coerce_extensions,
)


class TestHelperFunctions:
    """Testes para funções auxiliares do extractor."""
    
    def test_normalize_text_basic(self):
        """Testa normalização básica de texto."""
        assert _normalize_text("Olá Mundo") == "ola mundo"
        assert _normalize_text("TESTE") == "teste"
        assert _normalize_text("  espaços  ") == "espacos"
    
    def test_normalize_text_empty(self):
        """Testa normalização com valores vazios."""
        assert _normalize_text("") == ""
        assert _normalize_text(None) == ""
    
    def test_coerce_extensions_string(self):
        """Testa conversão de extensões de string."""
        assert _coerce_extensions(".zip") == {".zip"}
        assert _coerce_extensions("zip") == {".zip"}
        assert _coerce_extensions("ZIP") == {".zip"}
    
    def test_coerce_extensions_list(self):
        """Testa conversão de extensões de lista."""
        result = _coerce_extensions([".zip", "csv", ".TXT"])
        assert result == {".zip", ".csv", ".txt"}
    
    def test_coerce_extensions_empty(self):
        """Testa conversão com valores vazios."""
        assert _coerce_extensions(None) == set()
        assert _coerce_extensions("") == set()
        assert _coerce_extensions([]) == set()


class TestVicEmailExtractor:
    """Testes para o extrator VIC via e-mail."""
    
    def test_init_without_config(self):
        """Testa inicialização sem config (usa padrão)."""
        with patch('src.core.extractor.load_cfg') as mock_cfg:
            mock_cfg.return_value = {"email": {"imap_server": "imap.gmail.com"}}
            extractor = VicEmailExtractor()
            assert extractor.config is not None
    
    def test_init_with_config(self):
        """Testa inicialização com config customizado."""
        config = {"email": {"imap_server": "custom.server.com"}}
        extractor = VicEmailExtractor(config)
        assert extractor.config == config


class TestMaxDBExtractor:
    """Testes para o extrator MAX via SQL Server."""
    
    def test_init_without_config(self):
        """Testa inicialização sem config."""
        with patch('src.core.extractor.load_cfg') as mock_cfg:
            mock_cfg.return_value = {"max": {"output_dir": "data/input/max"}}
            extractor = MaxDBExtractor()
            assert extractor.config is not None
    
    def test_init_with_config(self):
        """Testa inicialização com config customizado."""
        config = {"max": {"output_dir": "custom/path"}}
        extractor = MaxDBExtractor(config)
        assert extractor.config == config


class TestJudicialDBExtractor:
    """Testes para o extrator Judicial."""
    
    def test_init_without_config(self):
        """Testa inicialização sem config."""
        with patch('src.core.extractor.load_cfg') as mock_cfg:
            mock_cfg.return_value = {"judicial": {"output_dir": "data/input/judicial"}}
            extractor = JudicialDBExtractor()
            assert extractor.config is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
