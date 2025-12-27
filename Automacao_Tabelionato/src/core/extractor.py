#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Módulo consolidado de extração de bases para Tabelionato.

Consolida os scripts de extração em classes reutilizáveis:
- EmailDownloader: Download de emails via IMAP
- TabelionatoFileProcessor: Processamento de arquivos TXT/CSV
- TabelionatoEmailExtractor: Orquestra download + processamento
- MaxDBExtractor: Extração via SQL Server
"""

from __future__ import annotations

import csv
import imaplib
import email
import os
import re
import time
import zipfile
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from email.header import decode_header
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
from dotenv import load_dotenv

from src.utils.archives import ensure_7zip_ready, extract_with_7zip
from src.utils.helpers import (
    format_duration,
    format_int,
    generate_timestamp,
    normalize_ascii_lower,
    normalize_bool,
    normalize_cep,
    normalize_currency,
    normalize_data_tabelionato,
    normalize_text,
    print_section,
    suppress_console_info,
)
from src.utils.logger_config import get_logger


# =============================================================================
# DATACLASSES
# =============================================================================

@dataclass
class ExtracaoResumo:
    """Resumo da extração de dados."""
    data_email: Optional[str] = None
    email_id: Optional[str] = None
    anexos_baixados: List[str] = field(default_factory=list)
    cobranca_arquivo: Optional[Path] = None
    cobranca_registros: int = 0
    custas_arquivo: Optional[Path] = None
    custas_registros: int = 0
    mensagem: Optional[str] = None


# =============================================================================
# CLASSE BASE
# =============================================================================

class BaseExtractor(ABC):
    """Classe base para extratores."""

    def __init__(self, base_path: Optional[Path] = None):
        self.base_path = base_path or Path(__file__).resolve().parents[2]
        self.logger = get_logger(self.__class__.__name__)

    @abstractmethod
    def extrair(self) -> Tuple[Optional[Path], int]:
        """Executa a extração e retorna (caminho_arquivo, num_registros)."""
        pass


# =============================================================================
# EMAIL DOWNLOADER
# =============================================================================

class EmailDownloader:
    """Classe para download automático de emails com anexos do Tabelionato."""

    def __init__(self, base_path: Path):
        self.base_path = base_path
        self.logger = get_logger("EmailDownloader")

        # Carregar variáveis de ambiente
        load_dotenv(base_path / ".env")

        self.email_user = os.getenv('EMAIL_USER')
        self.email_password = os.getenv('EMAIL_APP_PASSWORD')
        self.imap_server = os.getenv('IMAP_SERVER', 'imap.gmail.com')
        self.email_sender = 'adriano@4protestobh.com'

        # Tokens de busca
        self.subject_keyword = os.getenv(
            'EMAIL_SUBJECT_KEYWORD',
            'Base de Dados e Relatório de Recebimento de Custas Postergadas do 4 Tabelionato'
        )
        raw_tokens = os.getenv(
            'EMAIL_SUBJECT_TOKENS',
            'base de dados;relatorio de recebimento de custas;tabelionato'
        )
        self.subject_tokens = [
            normalize_ascii_lower(token)
            for token in raw_tokens.split(';')
            if token.strip()
        ] or [normalize_ascii_lower(self.subject_keyword)]

        # Diretórios
        self.input_dir = base_path / "data" / "input" / "tabelionato"
        self.input_dir_custas = base_path / "data" / "input" / "tabelionato custas"

        # Validar configurações
        if not all([self.email_user, self.email_password]):
            raise ValueError("Credenciais de email não configuradas no .env")

        self.logger.info("Conta IMAP: %s", self.email_user)
        self.logger.info("Remetente: %s", self.email_sender)

    def conectar_imap(self) -> imaplib.IMAP4_SSL:
        """Conecta ao servidor IMAP."""
        self.logger.info("Conectando ao servidor IMAP: %s", self.imap_server)
        mail = imaplib.IMAP4_SSL(self.imap_server)
        mail.login(self.email_user, self.email_password)
        self.logger.info("Conexão IMAP estabelecida")
        return mail

    def _decodificar_header(self, header_value: str) -> str:
        """Decodifica header de email."""
        if header_value is None:
            return ""
        decoded_parts = decode_header(header_value)
        result = ""
        for part, encoding in decoded_parts:
            if isinstance(part, bytes):
                result += part.decode(encoding or 'utf-8', errors='ignore')
            else:
                result += part
        return result

    def buscar_emails_recentes(self, mail: imaplib.IMAP4_SSL) -> List[str]:
        """Busca emails do remetente especificado."""
        mail.select('INBOX')
        criterio = f'(FROM "{self.email_sender}")'
        self.logger.info("Buscando emails: %s", criterio)

        status, messages = mail.search(None, criterio)
        if status != 'OK':
            return []

        email_ids = messages[0].split()
        self.logger.info("Encontrados %d emails", len(email_ids))
        return [eid.decode() for eid in email_ids]

    def processar_email(self, mail: imaplib.IMAP4_SSL, email_id: str) -> Optional[Tuple[List[str], str]]:
        """Processa email e baixa anexos relevantes."""
        status, msg_data = mail.fetch(email_id, '(RFC822)')
        if status != 'OK':
            return None

        email_message = email.message_from_bytes(msg_data[0][1])
        assunto = self._decodificar_header(email_message['Subject'])

        # Verificar tokens obrigatórios
        assunto_norm = normalize_ascii_lower(assunto)
        missing = [t for t in self.subject_tokens if t and t not in assunto_norm]
        if missing:
            self.logger.info("Email ignorado - tokens ausentes: %s", missing)
            return None

        # Extrair data do email
        try:
            data_obj = parsedate_to_datetime(email_message['Date'])
            data_email = data_obj.strftime("%Y-%m-%d")
        except Exception:
            self.logger.warning("Erro ao extrair data do email")
            return None

        # Processar anexos
        arquivos = self._processar_anexos(email_message)
        if arquivos:
            return arquivos, data_email
        return None

    def _processar_anexos(self, email_message) -> List[str]:
        """Processa e salva anexos do email."""
        arquivos_salvos = []

        for part in email_message.walk():
            if part.get_content_disposition() != 'attachment':
                continue

            filename = self._decodificar_header(part.get_filename() or "")
            if not filename:
                continue

            filename_lower = filename.lower()

            # Anexo Cobrança
            if (('cobranca' in filename_lower or 'cobrana' in filename_lower) and
                'recebimento' not in filename_lower and
                filename_lower.endswith(('.zip', '.rar'))):

                caminho = self._salvar_anexo(
                    part, 'cobranca', filename,
                    self.input_dir, "tabelionato_cobranca.*"
                )
                if caminho:
                    arquivos_salvos.append(str(caminho))

            # Anexo Custas
            elif ('recebimento' in filename_lower and 'custas' in filename_lower and
                  filename_lower.endswith(('.csv', '.zip', '.rar'))):

                caminho = self._salvar_anexo(
                    part, 'custas', filename,
                    self.input_dir_custas, "RecebimentoCustas_*.*"
                )
                if caminho:
                    arquivos_salvos.append(str(caminho))

        return arquivos_salvos

    def _salvar_anexo(
        self, part, tipo: str, filename: str, diretorio: Path, padrao_limpeza: str
    ) -> Optional[Path]:
        """Salva anexo no diretório especificado."""
        try:
            diretorio.mkdir(parents=True, exist_ok=True)

            # Limpar arquivos anteriores
            for antigo in diretorio.glob(padrao_limpeza):
                antigo.unlink()

            # Determinar nome do arquivo
            if tipo == 'cobranca':
                extensao = Path(filename).suffix.lower()
                nome = f"tabelionato_cobranca{extensao}"
            else:
                nome = filename

            caminho = diretorio / nome
            with open(caminho, 'wb') as f:
                f.write(part.get_payload(decode=True))

            self.logger.info("Anexo %s salvo: %s", tipo, caminho)
            return caminho
        except Exception as e:
            self.logger.error("Erro ao salvar anexo %s: %s", tipo, e)
            return None

    def baixar_emails(self, dias: int = 7) -> List[Tuple[str, str]]:
        """Baixa emails do Tabelionato."""
        arquivos_baixados = []

        try:
            mail = self.conectar_imap()
            email_ids = self.buscar_emails_recentes(mail)

            if email_ids:
                # Processar apenas o mais recente
                resultado = self.processar_email(mail, email_ids[-1])
                if resultado:
                    arquivos, data_hora = resultado
                    for arq in arquivos:
                        arquivos_baixados.append((arq, data_hora))

            mail.close()
            mail.logout()
        except Exception as e:
            self.logger.error("Erro no download: %s", e)

        return arquivos_baixados


# =============================================================================
# PROCESSADOR DE ARQUIVOS
# =============================================================================

class TabelionatoFileProcessor:
    """Processador de arquivos TXT/CSV do Tabelionato."""

    COLUMN_NAMES = [
        'Protocolo', 'VrTitulo', 'DtAnuencia', 'Devedor',
        'Endereco', 'Cidade', 'Cep', 'CpfCnpj',
        'Intimado', 'Custas', 'Credor'
    ]

    def __init__(self, base_path: Path):
        self.base_path = base_path
        self.logger = get_logger("TabelionatoFileProcessor")
        self.input_dir = base_path / "data" / "input" / "tabelionato"
        self.input_dir_custas = base_path / "data" / "input" / "tabelionato custas"

    def processar_cobranca(self, txt_path: Path, data_email: str, debug: bool = False) -> Optional[Tuple[Path, int]]:
        """Processa arquivo TXT de cobrança e retorna (zip_path, registros)."""
        try:
            self.logger.info("Processando cobrança: %s", txt_path)

            # Ler arquivo
            with open(txt_path, 'r', encoding='utf-8-sig', errors='ignore') as f:
                linhas = f.readlines()

            if not linhas:
                self.logger.error("Arquivo vazio")
                return None

            # Detectar formato e parsear
            registros = self._parse_linhas(linhas)
            if not registros:
                self.logger.error("Nenhum registro válido")
                return None

            # Criar DataFrame
            df = pd.DataFrame(registros, columns=self.COLUMN_NAMES)
            df = self._normalizar_dataframe(df)
            df = self._extrair_campos_credor(df)

            # Adicionar data
            df['DataExtracao'] = data_email

            # Exportar
            zip_path = self._exportar_zip(df, self.input_dir, "Tabelionato")

            self.logger.info("Cobrança processada: %d registros", len(df))
            return zip_path, len(df)

        except Exception as e:
            self.logger.error("Erro ao processar cobrança: %s", e)
            return None

    def processar_custas(self, txt_path: Path, data_email: str, debug: bool = False) -> Optional[Tuple[Path, int]]:
        """Processa arquivo CSV de custas e retorna (zip_path, registros)."""
        try:
            self.logger.info("Processando custas: %s", txt_path)

            # Ler arquivo
            if txt_path.suffix.lower() == '.csv':
                df = pd.read_csv(txt_path, encoding='utf-8-sig', sep=';')
            else:
                df = pd.read_csv(txt_path, encoding='utf-8-sig', sep='\t')

            # Tratar Protocolo
            if 'Protocolo' in df.columns:
                df['Protocolo'] = df['Protocolo'].astype(str).str.replace(r'[^\w]', '', regex=True)

            # Calcular Valor Total Pago
            df = self._calcular_valor_total_pago(df)

            # Adicionar data
            df['DataExtracao'] = data_email

            # Exportar
            timestamp = generate_timestamp()
            zip_path = self._exportar_zip(df, self.input_dir_custas, f"RecebimentoCustas_{timestamp}")

            self.logger.info("Custas processadas: %d registros", len(df))
            return zip_path, len(df)

        except Exception as e:
            self.logger.error("Erro ao processar custas: %s", e)
            return None

    def _parse_linhas(self, linhas: List[str]) -> List[List[str]]:
        """Parseia linhas do arquivo TXT."""
        registros = []

        # Detectar header
        header_line = linhas[0].rstrip('\r\n') if linhas else ''
        start_idx = 1 if header_line and 'Protocolo' in header_line else 0

        # Detectar formato (largura fixa ou CSV)
        fixed_specs = self._compute_fixed_width_specs(header_line)

        for raw in linhas[start_idx:]:
            if not raw.strip():
                continue

            # Tentar parsear
            parsed = None
            if fixed_specs and ';' not in raw:
                parsed = self._parse_fixed_width(raw, fixed_specs)
            if parsed is None:
                parsed = self._parse_semicolon(raw)

            if parsed and len(parsed) == len(self.COLUMN_NAMES) and parsed[0]:
                registros.append(parsed)

        return registros

    def _compute_fixed_width_specs(self, header: str) -> Optional[List[Tuple[int, int]]]:
        """Calcula especificações de largura fixa a partir do header."""
        if not header or ';' in header:
            return None
        try:
            positions = [header.index(col) for col in self.COLUMN_NAMES]
            specs = []
            for idx, start in enumerate(positions):
                end = positions[idx + 1] if idx + 1 < len(positions) else len(header)
                specs.append((start, end))
            return specs
        except ValueError:
            return None

    def _parse_fixed_width(self, line: str, specs: List[Tuple[int, int]]) -> Optional[List[str]]:
        """Parseia linha de largura fixa."""
        line = line.rstrip('\r\n')
        if not line.strip():
            return None
        last_end = specs[-1][1]
        if len(line) < last_end:
            line = line.ljust(last_end)
        return [normalize_text(line[start:end]) for start, end in specs]

    def _parse_semicolon(self, line: str) -> Optional[List[str]]:
        """Parseia linha CSV com ponto-e-vírgula."""
        if ';' not in line:
            return None
        try:
            row = next(csv.reader([line], delimiter=';'))
            if len(row) != len(self.COLUMN_NAMES):
                return None
            return [normalize_text(v) for v in row]
        except Exception:
            return None

    def _normalizar_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica normalizações ao DataFrame."""
        df = df.fillna('')
        df['DtAnuencia'] = df['DtAnuencia'].apply(normalize_data_tabelionato)
        df['Cep'] = df['Cep'].apply(normalize_cep)
        df['CpfCnpj'] = df['CpfCnpj'].apply(normalize_text)
        df['Intimado'] = df['Intimado'].apply(normalize_bool)
        df['Custas'] = df['Custas'].apply(normalize_currency)
        df['Credor'] = df['Credor'].apply(normalize_text)
        df['Endereco'] = df['Endereco'].apply(normalize_text)
        df['Cidade'] = df['Cidade'].apply(normalize_text)
        return df

    def _extrair_campos_credor(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extrai campos faltantes da coluna Credor."""
        # Extrair Intimado do Credor
        mask_int = df['Intimado'].eq('')
        bool_extract = df.loc[mask_int, 'Credor'].str.extract(
            r'\b(False|True|FALSO|VERDADEIRO)\b', flags=re.IGNORECASE
        )[0]
        if bool_extract is not None:
            found = bool_extract.notna()
            if found.any():
                idxs = bool_extract[found].index
                df.loc[idxs, 'Intimado'] = bool_extract[found].apply(normalize_bool)
                df.loc[idxs, 'Credor'] = df.loc[idxs, 'Credor'].str.replace(
                    r'\b(False|True|FALSO|VERDADEIRO)\b', '', n=1, regex=True
                ).str.strip()

        # Extrair CPF/CNPJ do Credor
        mask_cpf = df['CpfCnpj'].eq('')
        cpf_extract = df.loc[mask_cpf, 'Credor'].str.extract(
            r'(\d{3}\.\d{3}\.\d{3}-\d{2}|\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2})'
        )[0]
        if cpf_extract is not None:
            found = cpf_extract.notna()
            if found.any():
                idxs = cpf_extract[found].index
                df.loc[idxs, 'CpfCnpj'] = cpf_extract[found].apply(normalize_text)
                df.loc[idxs, 'Credor'] = df.loc[idxs, 'Credor'].str.replace(
                    r'(\d{3}\.\d{3}\.\d{3}-\d{2}|\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2})',
                    '', n=1, regex=True
                ).str.strip()

        # Extrair Custas do Credor
        mask_custas = df['Custas'].eq('') & df['Credor'].str.contains(r'R\$', na=False)
        if mask_custas.any():
            custas_extract = df.loc[mask_custas, 'Credor'].str.extract(r'(R\$\s*[0-9.,]+)')[0]
            df.loc[mask_custas, 'Custas'] = custas_extract.apply(normalize_currency)
            df.loc[mask_custas, 'Credor'] = df.loc[mask_custas, 'Credor'].str.replace(
                r'R\$\s*[0-9.,]+\s*', '', regex=True
            ).str.strip()

        return df

    def _calcular_valor_total_pago(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calcula coluna Valor Total Pago."""
        custas_col = None
        cancel_col = None

        for col in df.columns:
            col_lower = col.lower()
            if 'custas' in col_lower and 'pago' in col_lower:
                custas_col = col
            elif 'cancelamento' in col_lower and 'pago' in col_lower:
                cancel_col = col

        if custas_col and cancel_col:
            def converter(serie):
                def limpar(v):
                    if pd.isna(v) or v == '':
                        return Decimal('0.00')
                    s = str(v).replace('R$', '').replace(' ', '').strip()
                    if ',' in s:
                        partes = s.split(',')
                        if len(partes) == 2:
                            s = partes[0].replace('.', '') + '.' + partes[1][:2]
                    try:
                        return Decimal(s) if s else Decimal('0.00')
                    except InvalidOperation:
                        return Decimal('0.00')
                return serie.apply(limpar)

            df['Valor Total Pago'] = converter(df[custas_col]) + converter(df[cancel_col])

        return df

    def _exportar_zip(self, df: pd.DataFrame, diretorio: Path, nome_base: str) -> Path:
        """Exporta DataFrame para ZIP."""
        diretorio.mkdir(parents=True, exist_ok=True)

        csv_name = f"{nome_base}.csv"
        zip_name = f"{nome_base}.zip"
        zip_path = diretorio / zip_name
        temp_csv = diretorio / csv_name

        df.to_csv(temp_csv, index=False, sep=';', encoding='utf-8-sig')

        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.write(temp_csv, csv_name)

        temp_csv.unlink(missing_ok=True)
        return zip_path


# =============================================================================
# EXTRATOR PRINCIPAL
# =============================================================================

class TabelionatoEmailExtractor(BaseExtractor):
    """Extrator consolidado do Tabelionato via email IMAP."""

    def __init__(self, base_path: Optional[Path] = None, dias: int = 7, debug: bool = False):
        super().__init__(base_path)
        self.dias = dias
        self.debug = debug
        self.input_dir = self.base_path / "data" / "input" / "tabelionato"
        self.input_dir_custas = self.base_path / "data" / "input" / "tabelionato custas"

    def extrair(self) -> ExtracaoResumo:
        """Executa extração completa do Tabelionato."""
        inicio = time.perf_counter()
        resumo = ExtracaoResumo()

        suppress_console_info(self.logger)
        ensure_7zip_ready()

        # 1. Download de emails
        data_email = self._baixar_emails(resumo)

        if not data_email:
            self._exibir_erro(resumo)
            return resumo

        # 2. Processar arquivos
        processor = TabelionatoFileProcessor(self.base_path)

        self._processar_cobranca(processor, data_email, resumo)
        self._processar_custas(processor, data_email, resumo)

        # 3. Exibir resumo
        duracao = time.perf_counter() - inicio
        self._exibir_resumo(resumo, duracao)

        return resumo

    def _baixar_emails(self, resumo: ExtracaoResumo) -> Optional[str]:
        """Baixa emails e retorna data do email."""
        try:
            self.logger.info("Baixando emails...")
            downloader = EmailDownloader(self.base_path)
            arquivos = downloader.baixar_emails(self.dias)

            if arquivos:
                resumo.anexos_baixados = [Path(a).name for a, _ in arquivos]
                _, data_email = arquivos[0]
                resumo.data_email = data_email
                return data_email
            else:
                resumo.mensagem = "Nenhum email novo encontrado."
                return None
        except Exception as e:
            resumo.mensagem = f"Falha no download: {e}"
            self.logger.error(resumo.mensagem)
            return None

    def _processar_cobranca(self, processor: TabelionatoFileProcessor, data_email: str, resumo: ExtracaoResumo):
        """Processa arquivo de cobrança."""
        self.logger.info("=" * 60)
        self.logger.info("   PROCESSANDO COBRANÇA")
        self.logger.info("=" * 60)

        arquivo = self._encontrar_arquivo(self.input_dir, "tabelionato_cobranca.*", ['*.zip', '*.rar', '*.txt', '*.csv'])
        if not arquivo:
            return

        txt_path = self._extrair_se_necessario(arquivo, self.input_dir)
        if not txt_path:
            return

        resultado = processor.processar_cobranca(txt_path, data_email, self.debug)

        # Cleanup
        if not self.debug and txt_path != arquivo and txt_path.exists():
            txt_path.unlink()
        if resultado and not self.debug and arquivo.exists() and arquivo != resultado[0]:
            arquivo.unlink()

        if resultado:
            resumo.cobranca_arquivo, resumo.cobranca_registros = resultado

    def _processar_custas(self, processor: TabelionatoFileProcessor, data_email: str, resumo: ExtracaoResumo):
        """Processa arquivo de custas."""
        self.logger.info("=" * 60)
        self.logger.info("   PROCESSANDO CUSTAS")
        self.logger.info("=" * 60)

        arquivo = self._encontrar_arquivo(self.input_dir_custas, "RecebimentoCustas_*.*", [])
        if not arquivo:
            return

        txt_path = self._extrair_se_necessario(arquivo, self.input_dir_custas)
        if not txt_path:
            return

        resultado = processor.processar_custas(txt_path, data_email, self.debug)

        # Cleanup
        if not self.debug and txt_path != arquivo and txt_path.exists():
            txt_path.unlink()
        if resultado and not self.debug and arquivo.exists() and arquivo != resultado[0]:
            arquivo.unlink()

        if resultado:
            resumo.custas_arquivo, resumo.custas_registros = resultado

    def _encontrar_arquivo(self, diretorio: Path, padrao_principal: str, padroes_fallback: List[str]) -> Optional[Path]:
        """Encontra arquivo no diretório."""
        arquivos = sorted(diretorio.glob(padrao_principal))
        if arquivos:
            return arquivos[0]

        for padrao in padroes_fallback:
            candidatos = sorted(diretorio.glob(padrao))
            if candidatos:
                return candidatos[0]

        self.logger.warning("Nenhum arquivo encontrado em %s", diretorio)
        return None

    def _extrair_se_necessario(self, arquivo: Path, destino: Path) -> Optional[Path]:
        """Extrai arquivo se for ZIP/RAR, senão retorna o próprio."""
        suffix = arquivo.suffix.lower()

        if suffix in {'.txt', '.csv'}:
            return arquivo

        if suffix in {'.zip', '.rar'}:
            return extrair_arquivos_compactados(arquivo, destino)

        self.logger.error("Formato não suportado: %s", suffix)
        return None

    def _exibir_erro(self, resumo: ExtracaoResumo):
        """Exibe mensagem de erro."""
        linhas = [
            "[ERRO] Extração Tabelionato não concluída.",
            "",
            resumo.mensagem or "Data do email não encontrada.",
        ]
        print_section("EXTRAÇÃO - TABELIONATO", linhas, leading_break=False)

    def _exibir_resumo(self, resumo: ExtracaoResumo, duracao: float):
        """Exibe resumo da extração."""
        anexos = ", ".join(resumo.anexos_baixados) if resumo.anexos_baixados else "-"
        linhas = [
            "[STEP] Extração Tabelionato",
            "",
            f"Email processado: {resumo.data_email or '-'}",
            f"Anexos baixados: {anexos}",
            "",
            f"Cobrança: {format_int(resumo.cobranca_registros)} registros",
            f"Arquivo cobrança: {resumo.cobranca_arquivo or '-'}",
            "",
            f"Custas: {format_int(resumo.custas_registros)} registros",
            f"Arquivo custas: {resumo.custas_arquivo or '-'}",
            "",
            f"Duração: {format_duration(duracao)}",
        ]
        print_section("EXTRAÇÃO - TABELIONATO", linhas, leading_break=False)


# =============================================================================
# EXTRATOR MAX
# =============================================================================

class MaxDBExtractor(BaseExtractor):
    """Extrator de base MAX via SQL Server."""

    def __init__(self, base_path: Optional[Path] = None, profile: str = "tabelionato"):
        super().__init__(base_path)
        self.profile = profile

    def extrair(self) -> Tuple[Optional[Path], int]:
        """Executa extração MAX do SQL Server."""
        from scripts.extrair_basemax import extract_max_tabelionato_data

        inicio = time.time()
        zip_path, registros = extract_max_tabelionato_data(self.profile)
        duracao = time.time() - inicio

        self.logger.info("Extração MAX: %d registros em %.2fs", registros, duracao)
        return zip_path, registros


# =============================================================================
# FUNÇÕES AUXILIARES
# =============================================================================

def extrair_arquivos_compactados(
    arquivo_path: Path,
    destino: Path,
    senha: str = "Mf4tab@"
) -> Optional[Path]:
    """Função unificada para extrair arquivos ZIP/RAR."""
    logger = get_logger("extractor")

    arquivo_path = Path(arquivo_path)
    if not arquivo_path.exists():
        logger.error("Arquivo não encontrado: %s", arquivo_path)
        return None

    try:
        novos_arquivos = extract_with_7zip(arquivo_path, destino, senha=senha)
    except FileNotFoundError as exc:
        logger.error("7-Zip não localizado: %s", exc)
        return None
    except RuntimeError as exc:
        logger.error("Falha ao extrair: %s", exc)
        return None

    for arquivo in novos_arquivos:
        if arquivo.suffix.lower() in {'.txt', '.csv'}:
            logger.info("Arquivo extraído: %s", arquivo)
            return arquivo

    logger.error("Nenhum TXT/CSV encontrado após extração.")
    return None


__all__ = [
    "BaseExtractor",
    "EmailDownloader",
    "TabelionatoFileProcessor",
    "TabelionatoEmailExtractor",
    "MaxDBExtractor",
    "ExtracaoResumo",
    "extrair_arquivos_compactados",
]
