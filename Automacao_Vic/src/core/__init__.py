"""Módulo core - Classes base e utilitários compartilhados."""

from .base_processor import BaseProcessor
from .file_manager import FileManager
from .packager import ExportacaoService, criar_servico_exportacao

__all__ = ['BaseProcessor', 'FileManager', 'ExportacaoService', 'criar_servico_exportacao']
