#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Módulo consolidado de extração de bases para EMCCAMP.

Consolida os scripts de extração em classes reutilizáveis:
- EmccampExtractor: Extração via API TOTVS
- MaxDBExtractor: Extração via SQL Server
- JudicialDBExtractor: Extração Autojur + MAX Smart
- BaixaExtractor: Extração de baixas via API TOTVS
- DoublecheckExtractor: Extração de acordos em aberto
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Tuple, Optional

import pandas as pd
from dotenv import load_dotenv

from src.config.loader import ConfigLoader, LoadedConfig
from src.utils.io import write_csv_to_zip
from src.utils.queries import get_query
from src.utils.sql_conn import get_candiotto_connection, get_std_connection
from src.utils.totvs_client import baixar_emccamp, baixar_baixas_emccamp
from src.utils.output_formatter import (
    OutputFormatter,
    format_extraction_output,
    format_extraction_judicial_output,
)


class BaseExtractor(ABC):
    """Classe base para extratores."""

    def __init__(self, config: Optional[LoadedConfig] = None, base_path: Optional[Path] = None):
        if config is not None:
            self.config = config
            self.base_path = getattr(config, 'base_path', base_path or Path.cwd())
        else:
            self.base_path = base_path or Path(__file__).resolve().parents[2]
            load_dotenv(self.base_path / ".env")
            loader = ConfigLoader(base_path=self.base_path)
            self.config = loader.load()

    @abstractmethod
    def extrair(self) -> Tuple[Optional[Path], int]:
        """Executa a extração e retorna (caminho_arquivo, num_registros)."""
        pass


class EmccampExtractor(BaseExtractor):
    """Extrator de base EMCCAMP via API TOTVS."""

    def extrair(self) -> Tuple[Optional[Path], int]:
        """Extrai dados EMCCAMP via API TOTVS."""
        inicio = time.time()
        zip_path, records = baixar_emccamp(self.config)
        duracao = time.time() - inicio

        format_extraction_output(
            source="EMCCAMP (API TOTVS)",
            output_file=str(zip_path),
            records=records,
            duration=duracao,
            steps=[
                "Conexão com API TOTVS",
                f"Download de {OutputFormatter.format_count(records)} registros",
                "Conversão para DataFrame",
                f"Salvamento em {zip_path.name}"
            ]
        )
        return zip_path, records


class MaxDBExtractor(BaseExtractor):
    """Extrator de base MAX via SQL Server."""

    def extrair(self) -> Tuple[Optional[Path], int]:
        """Extrai dados MAX via SQL Server."""
        inicio = time.time()

        global_cfg = self.config.get("global", {})
        encoding = global_cfg.get("encoding", "utf-8-sig")
        csv_sep = global_cfg.get("csv_separator", ",")

        input_cfg = self.config.get("paths", {}).get("input", {})
        input_dir = Path(input_cfg.get("base_max", "data/input/base_max"))
        if not input_dir.is_absolute():
            input_dir = self.base_path / input_dir
        input_dir.mkdir(parents=True, exist_ok=True)

        # Limpar arquivos antigos
        for old_file in input_dir.glob("MaxSmart*.zip"):
            try:
                old_file.unlink()
            except Exception:
                pass

        conn = get_std_connection(self.base_path)
        if not conn.connect():
            print("\n[ERRO] Falha na conexão SQL")
            print("[INFO] Verifique VPN ou credenciais do banco de dados\n")
            return None, 0

        try:
            query = get_query(self.config, "max")
            df = conn.execute_query(query)
            if df is None or df.empty:
                print("[ERRO] Consulta não retornou registros.")
                return None, 0

            registros = len(df)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_name = f"MaxSmart_{timestamp}.csv"
            zip_path = input_dir / "MaxSmart.zip"
            write_csv_to_zip({csv_name: df}, zip_path, sep=csv_sep, encoding=encoding)

            duracao = time.time() - inicio
            format_extraction_output(
                source="MAX (SQL Server STD2016)",
                output_file=str(zip_path),
                records=registros,
                duration=duracao,
                steps=[
                    "Conexão com banco SQL Server",
                    "Execução de query MAX",
                    f"Processamento de {OutputFormatter.format_count(registros)} registros",
                    f"Salvamento em {zip_path.name}"
                ]
            )
            return zip_path, registros
        finally:
            conn.close()


class JudicialDBExtractor(BaseExtractor):
    """Extrator de bases judiciais (Autojur + MAX Smart)."""

    def _executar_query(self, conn_factory, nome_query: str) -> pd.DataFrame:
        """Executa query em conexão específica."""
        conn = conn_factory(self.base_path)
        if not conn.connect():
            print(f"\n[ERRO] Falha na conexão SQL para {nome_query}")
            print("[INFO] Verifique VPN ou credenciais do banco de dados\n")
            raise RuntimeError(f'Falha na conexão para consulta {nome_query}')
        try:
            query = get_query(self.config, nome_query)
            return conn.execute_query(query)
        finally:
            conn.close()

    def extrair(self) -> Tuple[Optional[Path], int]:
        """Extrai e combina dados Autojur + MAX Smart."""
        inicio = time.time()

        df_autojur = self._executar_query(get_candiotto_connection, 'autojur')
        df_maxsmart = self._executar_query(get_std_connection, 'maxsmart_judicial')

        df_final = pd.concat([df_autojur, df_maxsmart], ignore_index=True)
        df_final['CPF_CNPJ'] = df_final['CPF_CNPJ'].astype(str).str.replace(r'[^0-9]', '', regex=True)

        duplicatas = len(df_final) - len(df_final.drop_duplicates(subset=['CPF_CNPJ'], keep='first'))
        df_final = df_final.drop_duplicates(subset=['CPF_CNPJ'], keep='first')

        input_cfg = self.config.get('paths', {}).get('input', {})
        input_dir = Path(input_cfg.get('judicial', 'data/input/judicial'))
        input_dir = input_dir if input_dir.is_absolute() else self.base_path / input_dir
        input_dir.mkdir(parents=True, exist_ok=True)

        # Limpar arquivos antigos
        for arquivo_antigo in input_dir.glob('ClientesJudiciais*.zip'):
            try:
                arquivo_antigo.unlink()
            except Exception:
                pass

        global_cfg = self.config.get('global', {})
        sep = global_cfg.get('csv_separator', ',')
        encoding = global_cfg.get('encoding', 'utf-8-sig')

        timestamp = time.strftime('%Y%m%d_%H%M%S')
        csv_name = f'ClientesJudiciais_{timestamp}.csv'
        zip_path = input_dir / 'ClientesJudiciais.zip'
        write_csv_to_zip({csv_name: df_final}, zip_path, sep=sep, encoding=encoding)

        duracao = time.time() - inicio
        format_extraction_judicial_output(
            autojur_records=len(df_autojur),
            maxsmart_records=len(df_maxsmart),
            duplicates_removed=duplicatas,
            total_unique=len(df_final),
            output_file=str(zip_path),
            duration=duracao
        )
        return zip_path, len(df_final)


class BaixaExtractor(BaseExtractor):
    """Extrator de baixas EMCCAMP via API TOTVS."""

    def extrair(self) -> Tuple[Optional[Path], int]:
        """Extrai dados de baixas EMCCAMP."""
        inicio = time.time()
        zip_path, records = baixar_baixas_emccamp(self.config)
        duracao = time.time() - inicio

        format_extraction_output(
            source="BAIXAS EMCCAMP (API TOTVS)",
            output_file=str(zip_path),
            records=records,
            duration=duracao,
            steps=[
                "Conexão com API TOTVS",
                "Download de dados de baixas",
                "Filtro: HONORARIO_BAIXADO != 0",
                f"Resultado: {OutputFormatter.format_count(records)} registros",
                f"Salvamento em {zip_path.name}"
            ]
        )
        return zip_path, records


class DoublecheckExtractor(BaseExtractor):
    """Extrator de acordos em aberto para doublecheck."""

    def extrair(self) -> Tuple[Optional[Path], int]:
        """Extrai dados de acordos em aberto."""
        inicio = time.time()

        conn = get_std_connection(self.base_path)
        if not conn.connect():
            print("[ERRO] Falha ao conectar ao SQL Server para acordos.")
            return None, 0

        try:
            query = get_query(self.config, "doublecheck_acordo")
            df = conn.execute_query(query)
            if df is None:
                print("[ERRO] Consulta retornou resultado vazio.")
                return None, 0

            path_str = self.config.data.get("inputs", {}).get(
                "acordos_abertos_path", "data/input/doublecheck_acordo/acordos_abertos.zip"
            )
            zip_path = Path(path_str)
            if not zip_path.is_absolute():
                zip_path = self.base_path / zip_path
            zip_path.parent.mkdir(parents=True, exist_ok=True)
            if zip_path.exists():
                zip_path.unlink()

            global_cfg = self.config.get("global", {})
            sep = global_cfg.get("csv_separator", ";")
            encoding = global_cfg.get("encoding", "utf-8-sig")

            timestamp = time.strftime('%Y%m%d_%H%M%S')
            csv_name = f'acordos_abertos_{timestamp}.csv'
            write_csv_to_zip({csv_name: df}, zip_path, sep=sep, encoding=encoding)

            duracao = time.time() - inicio
            format_extraction_output(
                source="ACORDOS ABERTOS (Doublecheck)",
                output_file=str(zip_path),
                records=len(df),
                duration=duracao,
                steps=[
                    "Conexão com SQL Server",
                    "Query de acordos em aberto",
                    f"Exportação de {OutputFormatter.format_count(len(df))} registros",
                    f"Salvamento em {zip_path.name}"
                ]
            )
            return zip_path, len(df)
        finally:
            conn.close()


def extrair_todas_bases(config: Optional[LoadedConfig] = None) -> dict:
    """Função de conveniência para extrair todas as bases."""
    resultados = {}

    for nome, ExtractorClass in [
        ("emccamp", EmccampExtractor),
        ("max", MaxDBExtractor),
        ("judicial", JudicialDBExtractor),
        ("baixa", BaixaExtractor),
        ("doublecheck", DoublecheckExtractor),
    ]:
        try:
            extractor = ExtractorClass(config)
            caminho, registros = extractor.extrair()
            resultados[nome] = {"status": "sucesso", "arquivo": str(caminho), "registros": registros}
        except Exception as e:
            resultados[nome] = {"status": "erro", "erro": str(e)}

    return resultados


__all__ = [
    "BaseExtractor",
    "EmccampExtractor",
    "MaxDBExtractor",
    "JudicialDBExtractor",
    "BaixaExtractor",
    "DoublecheckExtractor",
    "extrair_todas_bases",
]
