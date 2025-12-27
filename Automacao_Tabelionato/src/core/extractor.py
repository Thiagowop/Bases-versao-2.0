#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Módulo consolidado de extração de bases para Tabelionato.

Consolida os scripts de extração em classes reutilizáveis:
- TabelionatoEmailExtractor: Extração via email IMAP + processamento TXT
- MaxDBExtractor: Extração via SQL Server
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, Tuple
from dataclasses import dataclass, field

from src.utils.logger_config import get_logger


@dataclass
class ExtracaoResumo:
    """Resumo da extração de dados."""
    data_email: Optional[str] = None
    email_id: Optional[str] = None
    anexos_baixados: list = field(default_factory=list)
    cobranca_arquivo: Optional[Path] = None
    cobranca_registros: int = 0
    custas_arquivo: Optional[Path] = None
    custas_registros: int = 0
    mensagem: Optional[str] = None


class BaseExtractor(ABC):
    """Classe base para extratores."""

    def __init__(self, base_path: Optional[Path] = None):
        self.base_path = base_path or Path(__file__).resolve().parents[2]
        self.logger = get_logger(self.__class__.__name__)

    @abstractmethod
    def extrair(self) -> Tuple[Optional[Path], int]:
        """Executa a extração e retorna (caminho_arquivo, num_registros)."""
        pass


class TabelionatoEmailExtractor(BaseExtractor):
    """Extrator de dados Tabelionato via email IMAP.
    
    Esta classe encapsula a lógica do scripts/extrair_email.py original.
    Delega para o script original para manter compatibilidade.
    """

    def __init__(self, base_path: Optional[Path] = None, dias: int = 7, debug: bool = False):
        super().__init__(base_path)
        self.dias = dias
        self.debug = debug

    def extrair(self) -> Tuple[Optional[Path], int]:
        """Executa extração de email do Tabelionato."""
        # Importa e delega para a função main do script original
        from scripts.extrair_email import main
        
        inicio = time.time()
        main()  # Usa a função main existente que já está otimizada
        duracao = time.time() - inicio
        
        self.logger.info("Extração concluída em %.2fs", duracao)
        
        # Retorna None pois a função main já faz todo o output
        return None, 0


class MaxDBExtractor(BaseExtractor):
    """Extrator de base MAX via SQL Server.
    
    Esta classe encapsula a lógica do scripts/extrair_basemax.py original.
    """

    def __init__(self, base_path: Optional[Path] = None, profile: str = "tabelionato"):
        super().__init__(base_path)
        self.profile = profile

    def extrair(self) -> Tuple[Optional[Path], int]:
        """Executa extração MAX do SQL Server."""
        from scripts.extrair_basemax import extract_max_tabelionato_data
        
        inicio = time.time()
        zip_path, registros = extract_max_tabelionato_data(self.profile)
        duracao = time.time() - inicio
        
        self.logger.info("Extração MAX concluída: %d registros em %.2fs", registros, duracao)
        return zip_path, registros


def extrair_arquivos_compactados(
    arquivo_path: Path,
    destino: Path,
    senha: str = "Mf4tab@"
) -> Optional[Path]:
    """Função unificada para extrair arquivos ZIP/RAR.
    
    Substitui as 4 funções duplicadas:
    - extrair_zip_com_senha
    - extrair_zip_com_senha_custas  
    - extrair_rar_com_senha
    - extrair_rar_com_senha_custas
    """
    from src.utils.archives import extract_with_7zip
    
    logger = get_logger("extractor")
    
    arquivo_path = Path(arquivo_path)
    if not arquivo_path.exists():
        logger.error("Arquivo não encontrado: %s", arquivo_path)
        return None

    try:
        novos_arquivos = extract_with_7zip(arquivo_path, destino, senha=senha)
    except FileNotFoundError as exc:
        logger.error("Ferramenta 7-Zip não localizada: %s", exc)
        return None
    except RuntimeError as exc:
        logger.error("Falha ao extrair arquivo: %s", exc)
        return None

    for arquivo in novos_arquivos:
        if arquivo.suffix.lower() in {'.txt', '.csv'}:
            logger.info("Arquivo extraído: %s", arquivo)
            return arquivo

    logger.error("Nenhum arquivo TXT/CSV encontrado após extração.")
    return None


__all__ = [
    "BaseExtractor",
    "TabelionatoEmailExtractor", 
    "MaxDBExtractor",
    "ExtracaoResumo",
    "extrair_arquivos_compactados",
]
