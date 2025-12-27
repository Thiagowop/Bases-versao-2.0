#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Módulo consolidado de extração de bases.

Classes:
- VicEmailExtractor: Extração VIC via IMAP (Gmail)
- MaxDBExtractor: Extração MAX via SQL Server
- JudicialDBExtractor: Extração Judicial (Autojur + MAX Smart)
"""

from __future__ import annotations

import email
import imaplib
import os
import time
import unicodedata
import warnings
import zipfile
from datetime import datetime
from email.header import decode_header, make_header
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Set

import pandas as pd
from dotenv import load_dotenv

# Suprimir avisos do pandas sobre conexões DBAPI2
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

# Carregar variáveis de ambiente
PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

from src.config.loader import load_cfg
from src.utils.sql_conn import get_std_connection, get_candiotto_connection
from src.utils.queries_sql import SQL_MAX, SQL_AUTOJUR, SQL_MAXSMART_JUDICIAL
from src.utils.helpers import generate_timestamp


# =============================================================================
# FUNÇÕES AUXILIARES COMUNS
# =============================================================================

def _normalize_text(value: Optional[str]) -> str:
    """Normaliza texto para comparação (lowercase, sem acentos)."""
    if not value:
        return ""
    normalized = unicodedata.normalize("NFKD", value)
    stripped = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return stripped.lower().strip()


def _decode_header(header: Optional[str]) -> str:
    """Decodifica header de e-mail."""
    if not header:
        return ""
    try:
        return str(make_header(decode_header(header)))
    except Exception:
        try:
            return header.encode("latin1").decode("utf-8")
        except Exception:
            return str(header)


def _coerce_extensions(raw: Any) -> Set[str]:
    """Converte extensões para set normalizado."""
    if not raw:
        return set()
    if isinstance(raw, str):
        items = [raw]
    else:
        try:
            items = list(raw)
        except TypeError:
            items = [str(raw)]
    cleaned = set()
    for item in items:
        if not item:
            continue
        ext = str(item).strip().lower()
        if not ext:
            continue
        if not ext.startswith('.'):
            ext = f'.{ext}'
        cleaned.add(ext)
    return cleaned


def _contar_registros(arquivo: Path) -> Tuple[Optional[int], Optional[str]]:
    """Conta registros em arquivo CSV ou ZIP."""
    try:
        if arquivo.suffix.lower() == ".zip":
            with zipfile.ZipFile(arquivo) as zf:
                csvs = [name for name in zf.namelist() if name.lower().endswith(".csv")]
                if not csvs:
                    return None, None
                alvo = csvs[0]
                with zf.open(alvo) as fh:
                    df = pd.read_csv(fh, sep=";", encoding="utf-8-sig")
                return len(df), alvo
        if arquivo.suffix.lower() in {".csv", ".txt"}:
            df = pd.read_csv(arquivo, sep=";", encoding="utf-8-sig")
            return len(df), arquivo.name
    except Exception:
        return None, None
    return None, None


# =============================================================================
# EXTRATOR VIC (EMAIL/IMAP)
# =============================================================================

class VicEmailExtractor:
    """Extração de anexos VIC via IMAP (Gmail)."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Inicializa o extrator.
        
        Args:
            config: Configurações do projeto (se None, carrega do config.yaml)
        """
        self.config = config or load_cfg()
        self.project_root = PROJECT_ROOT
    
    def extrair(self, profile: str = "email") -> Optional[Path]:
        """Executa extração de anexo do e-mail.
        
        Args:
            profile: Nome da seção do config.yaml com configurações de e-mail
            
        Returns:
            Path do arquivo baixado ou None se falhou
        """
        print("=" * 60)
        print("     EXTRAÇÃO DE ANEXOS – GMAIL IMAP (READ-ONLY)")
        print("=" * 60)
        print()
        
        email_cfg = self.config.get(profile, {})
        
        if not isinstance(email_cfg, dict) or 'imap_server' not in email_cfg:
            print(f"[ERRO] Configuração de e-mail '{profile}' não encontrada em config.yaml.")
            return None
        
        email_user = os.getenv("EMAIL_USER")
        email_app_password = os.getenv("EMAIL_APP_PASSWORD")
        
        if not email_user or not email_app_password:
            print("[ERRO] Variáveis EMAIL_USER/EMAIL_APP_PASSWORD ausentes no .env")
            return None
        
        imap_server = email_cfg.get("imap_server", "imap.gmail.com")
        imap_folder = email_cfg.get("imap_folder", "INBOX")
        email_sender = email_cfg.get("email_sender", "").strip()
        email_subject_keyword = email_cfg.get("email_subject_keyword", "").strip()
        attachment_filename = email_cfg.get("attachment_filename", "").strip()
        attachment_keyword = email_cfg.get("attachment_keyword", "").strip()
        attachment_extensions = _coerce_extensions(email_cfg.get("attachment_extensions"))
        download_dir = self.project_root / email_cfg.get("download_dir", "data/input/vic")
        output_filename = email_cfg.get("output_filename", "anexo_email.zip").strip()
        
        download_dir.mkdir(parents=True, exist_ok=True)
        
        inicio = time.time()
        mail: Optional[imaplib.IMAP4_SSL] = None
        anexos_encontrados = 0
        anexos_baixados = 0
        arquivo_final: Optional[Path] = None
        email_info: Dict[str, str] = {}
        
        sender_filter_norm = _normalize_text(email_sender)
        subject_filter_norm = _normalize_text(email_subject_keyword)
        filename_norm = _normalize_text(attachment_filename)
        keyword_norm = _normalize_text(attachment_keyword)
        
        try:
            mail = imaplib.IMAP4_SSL(imap_server, 993)
            mail.login(email_user, email_app_password)
            
            typ, _ = mail.select(imap_folder)
            if typ != "OK":
                print(f"[ERRO] Não foi possível selecionar a pasta {imap_folder}")
                return None
            
            print(f"[INFO] Filtros aplicados | FROM='{email_sender or '*'}' SUBJECT~='{email_subject_keyword or '*'}'")
            
            def buscar(*criteria: str) -> list:
                if not criteria:
                    return []
                status, data = mail.search("UTF-8", *criteria)
                if status != "OK" or not data:
                    return []
                blob = data[0]
                return blob.split() if blob else []
            
            ids_sender = buscar("FROM", f'"{email_sender}"') if email_sender else []
            ids_subject = buscar("SUBJECT", f'"{email_subject_keyword}"') if email_subject_keyword else []
            ids_combined: list = []
            if email_sender and email_subject_keyword:
                ids_combined = buscar(f'(FROM "{email_sender}" SUBJECT "{email_subject_keyword}")')
            
            sorter = lambda items: sorted(items, key=lambda b: int(b), reverse=True)
            ids_sender = sorter(ids_sender)
            ids_subject = sorter(ids_subject)
            ids_combined = sorter(ids_combined)
            
            if ids_combined:
                ids_to_process = [ids_combined[0]]
            elif ids_sender:
                ids_to_process = [ids_sender[0]]
            elif ids_subject:
                ids_to_process = [ids_subject[0]]
            else:
                print("[ERRO] Nenhum e-mail encontrado com os critérios informados.")
                return None
            
            for eid in ids_to_process:
                status, msg_data = mail.fetch(eid, "(RFC822)")
                if status != "OK":
                    continue
                msg_bytes = next((part[1] for part in msg_data if isinstance(part, tuple)), None)
                if not msg_bytes:
                    continue
                
                msg = email.message_from_bytes(msg_bytes)
                from_h = _decode_header(msg.get("From", ""))
                subj_h = _decode_header(msg.get("Subject", ""))
                date_h = _decode_header(msg.get("Date", ""))
                
                email_info = {"remetente": from_h, "assunto": subj_h, "data": date_h}
                
                from_norm = _normalize_text(from_h)
                if sender_filter_norm and sender_filter_norm not in from_norm:
                    continue
                subj_norm = _normalize_text(subj_h)
                if subject_filter_norm and subject_filter_norm not in subj_norm:
                    continue
                
                for part in msg.walk():
                    if part.get_content_maintype() == "multipart":
                        continue
                    if part.get("Content-Disposition") is None:
                        continue
                    
                    filename = _decode_header(part.get_filename() or "")
                    if not filename:
                        continue
                    filename_candidate_norm = _normalize_text(filename)
                    if filename_norm and filename_candidate_norm != filename_norm:
                        continue
                    if keyword_norm and keyword_norm not in filename_candidate_norm:
                        continue
                    if attachment_extensions:
                        extensao = Path(filename).suffix.lower()
                        if extensao not in attachment_extensions:
                            continue
                    
                    anexos_encontrados += 1
                    payload = part.get_payload(decode=True)
                    if payload is None:
                        continue
                    
                    filepath = download_dir / output_filename
                    with open(filepath, "wb") as fh:
                        fh.write(payload)
                    anexos_baixados += 1
                    arquivo_final = filepath
            
            mail.logout()
            
        except imaplib.IMAP4.error as exc:
            print(f"[ERRO] IMAP: {exc}")
            return None
        except Exception as exc:
            print(f"[ERRO] Falha durante a extração: {exc}")
            return None
        finally:
            try:
                if mail is not None:
                    mail.logout()
            except Exception:
                pass
        
        elapsed = time.time() - inicio
        
        if anexos_baixados == 0 or not arquivo_final:
            print(f"[INFO] Tempo de execução: {elapsed:.2f} segundos")
            print("[AVISO] Nenhum anexo foi salvo.")
            return None
        
        # Validação de tamanho
        tamanho_mb = arquivo_final.stat().st_size / (1024 * 1024)
        validation_cfg = email_cfg.get('validation', {})
        min_size_mb = validation_cfg.get('min_file_size_mb', 0)
        
        if min_size_mb > 0 and tamanho_mb < min_size_mb:
            print(f"[ERRO] Arquivo muito pequeno: {tamanho_mb:.2f} MB (mínimo: {min_size_mb:.2f} MB)")
            return None
        
        registros, _ = _contar_registros(arquivo_final)
        
        print(f"\n✅ VIC (Email) - Extração concluída com sucesso")
        print(f"   📥 Anexos encontrados: {anexos_encontrados}")
        print(f"   📥 Anexos baixados: {anexos_baixados}")
        if registros:
            print(f"   📊 Registros: {registros:,}")
        print(f"   📁 Arquivo: {arquivo_final}")
        print(f"   ⏱️ Tempo de execução: {elapsed:.2f} segundos")
        if email_info.get("data"):
            print(f"   📅 Data/hora do e-mail: {email_info.get('data')}")
        
        return arquivo_final


# =============================================================================
# EXTRATOR MAX (SQL SERVER)
# =============================================================================

class MaxDBExtractor:
    """Extração da base MAX via SQL Server."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Inicializa o extrator.
        
        Args:
            config: Configurações do projeto (se None, carrega do config.yaml)
        """
        self.config = config or load_cfg()
        self.project_root = PROJECT_ROOT
    
    def extrair(self, profile: str = "max") -> Optional[Path]:
        """Executa extração da base MAX do banco SQL Server.
        
        Args:
            profile: Nome da seção do config.yaml com configurações de saída
            
        Returns:
            Path do arquivo ZIP gerado ou None se falhou
        """
        print("=" * 60)
        print("     EXTRAÇÃO DE DADOS MAX DO BANCO SQL SERVER")
        print("=" * 60)
        print()
        
        max_cfg = self.config.get(profile, {})
        
        if not isinstance(max_cfg, dict) or not max_cfg:
            print(f"[ERRO] Configuração '{profile}' não encontrada em config.yaml.")
            return None
        
        output_dir = self.project_root / max_cfg.get('output_dir', 'data/input/max')
        output_filename = max_cfg.get('output_filename', 'MaxSmart.zip')
        output_dir.mkdir(parents=True, exist_ok=True)
        
        inicio = time.time()
        
        conn = get_std_connection()
        if not conn.connect():
            print('[ERRO] Falha na conexão com o banco de dados. Verifique VPN ou credenciais.')
            return None
        
        try:
            print('[EXEC] Executando consulta MAX no banco de dados...')
            df = conn.execute_query(SQL_MAX)
            if df is None or df.empty:
                print('[ERRO] Nenhum dado retornado pela consulta.')
                return None
            
            registros = len(df)
            timestamp = generate_timestamp()
            csv_name = f'MaxSmart_{timestamp}.csv'
            zip_path = output_dir / output_filename
            temp_csv = output_dir / csv_name
            
            df.to_csv(temp_csv, index=False, encoding='utf-8-sig', sep=';')
            
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                zf.write(temp_csv, csv_name)
            temp_csv.unlink()
            
            elapsed = time.time() - inicio
            
            print(f"\n✅ MAX (DB) - Extração concluída com sucesso")
            print(f"   📊 Registros extraídos: {registros:,}")
            print(f"   📁 Arquivo: {zip_path}")
            print(f"   ⏱️ Tempo de execução: {elapsed:.2f} segundos")
            
            return zip_path
            
        finally:
            conn.close()


# =============================================================================
# EXTRATOR JUDICIAL (SQL SERVER - AUTOJUR + MAX SMART)
# =============================================================================

class JudicialDBExtractor:
    """Extração consolidada das bases judiciais (Autojur + MAX Smart)."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Inicializa o extrator.
        
        Args:
            config: Configurações do projeto (se None, carrega do config.yaml)
        """
        self.config = config or load_cfg()
        self.project_root = PROJECT_ROOT
    
    def _executar_consulta(self, conector, sql: str, descricao: str) -> pd.DataFrame:
        """Executa uma consulta SQL."""
        print(f"[EXEC] {descricao}...")
        if not conector.connect():
            raise RuntimeError(f"Falha na conexão com {descricao}.")
        try:
            df = conector.execute_query(sql)
            total = 0 if df is None else len(df)
            print(f"[OK] {descricao}: {total} registros")
            if df is None:
                return pd.DataFrame(columns=["CPF_CNPJ", "ORIGEM"])
            return df
        finally:
            conector.close()
    
    def extrair(self) -> Optional[Path]:
        """Executa extração das bases judiciais.
        
        Returns:
            Path do arquivo ZIP gerado ou None se falhou
        """
        print("=" * 60)
        print("     EXTRAÇÃO DE DADOS JUDICIAIS")
        print("=" * 60)
        print()
        
        judicial_cfg = self.config.get("judicial", {})
        
        inicio = time.time()
        
        try:
            # Extrair de cada fonte
            conn_candiotto = get_candiotto_connection()
            df_autojur = self._executar_consulta(conn_candiotto, SQL_AUTOJUR, "Consulta AUTOJUR")
            
            conn_std = get_std_connection()
            df_max = self._executar_consulta(conn_std, SQL_MAXSMART_JUDICIAL, "Consulta MAX Smart Judicial")
            
            # Combinar e deduplicar
            print("[INFO] Combinando resultados...")
            combinados = pd.concat([df_autojur, df_max], ignore_index=True)
            
            if 'CPF_CNPJ' in combinados.columns:
                combinados['_CPF_DIGITO'] = combinados['CPF_CNPJ'].astype(str).str.replace(r"[^0-9]", "", regex=True)
                chave = '_CPF_DIGITO'
            else:
                chave = 'CPF_CNPJ'
            
            unicos = combinados.drop_duplicates(subset=[chave], keep='first')
            
            if '_CPF_DIGITO' in unicos.columns:
                unicos = unicos.drop(columns=['_CPF_DIGITO'])
            
            if unicos.empty:
                print('[AVISO] Nenhum dado judicial encontrado.')
                return None
            
            # Salvar
            timestamp = generate_timestamp()
            csv_name = f'ClientesJudiciais_{timestamp}.csv'
            
            output_dir = self.project_root / judicial_cfg.get('output_dir', 'data/input/judicial')
            zip_name = judicial_cfg.get('output_filename', 'ClientesJudiciais.zip')
            output_dir.mkdir(parents=True, exist_ok=True)
            
            csv_path = output_dir / csv_name
            zip_path = output_dir / zip_name
            
            unicos.to_csv(csv_path, index=False, encoding='utf-8-sig', sep=';')
            
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                zf.write(csv_path, csv_name)
            csv_path.unlink()
            
            elapsed = time.time() - inicio
            
            print(f"\n✅ Judicial - Extração concluída com sucesso")
            print(f"   📊 AUTOJUR: {len(df_autojur):,} registros")
            print(f"   📊 MAX Smart: {len(df_max):,} registros")
            print(f"   📊 Total único: {len(unicos):,} registros")
            print(f"   📁 Arquivo: {zip_path}")
            print(f"   ⏱️ Tempo de execução: {elapsed:.2f} segundos")
            
            return zip_path
            
        except Exception as exc:
            print(f"[ERRO] Falha durante a extração judicial: {exc}")
            return None


# =============================================================================
# FUNÇÃO DE CONVENIÊNCIA
# =============================================================================

def extrair_todas_bases(config: Optional[Dict[str, Any]] = None) -> Dict[str, Optional[Path]]:
    """Executa extração de todas as bases (VIC, MAX, Judicial).
    
    Args:
        config: Configurações do projeto
        
    Returns:
        Dicionário com paths dos arquivos extraídos
    """
    resultados = {}
    
    print("\n" + "=" * 60)
    print("           EXTRAÇÃO DE TODAS AS BASES")
    print("=" * 60 + "\n")
    
    # VIC (Email)
    print("\nExecutando extração VIC (Email)...")
    vic_extractor = VicEmailExtractor(config)
    resultados["vic"] = vic_extractor.extrair()
    
    # MAX (DB)
    print("\n\nExecutando extração MAX (DB)...")
    max_extractor = MaxDBExtractor(config)
    resultados["max"] = max_extractor.extrair()
    
    # Judicial (DB)
    print("\n\nExecutando extração Judicial (DB)...")
    judicial_extractor = JudicialDBExtractor(config)
    resultados["judicial"] = judicial_extractor.extrair()
    
    return resultados


__all__ = [
    "VicEmailExtractor",
    "MaxDBExtractor",
    "JudicialDBExtractor",
    "extrair_todas_bases",
]
