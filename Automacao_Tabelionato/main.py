#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Pipeline principal do projeto Tabelionato.

Entry point unificado para executar as etapas do pipeline:
- Extração (Email + MAX)
- Tratamento (Tabelionato + MAX)
- Batimento
- Baixa
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

# Configuração UTF-8 para Windows
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

from src.utils.logger_config import get_logger, log_session_start, log_session_end
from src.utils.helpers import print_section, format_duration
from src.utils.console_runner import run_with_console

logger = get_logger("main")


def extrair_email():
    """Executa extração de email (Tabelionato)."""
    from scripts.extrair_email import main as extrair_email_main
    return extrair_email_main()


def extrair_max():
    """Executa extração MAX."""
    from scripts.extrair_basemax import main as extrair_max_main
    return extrair_max_main(profile="tabelionato")


def tratar_tabelionato():
    """Executa tratamento Tabelionato."""
    from src.processors.tabelionato import main as tratar_tabelionato_main
    return tratar_tabelionato_main()


def tratar_max():
    """Executa tratamento MAX."""
    from src.processors.max import main as tratar_max_main
    return tratar_max_main()


def executar_batimento():
    """Executa batimento Tabelionato x MAX."""
    from src.processors.batimento import main as batimento_main
    return batimento_main()


def executar_baixa():
    """Executa baixa."""
    from src.processors.baixa import main as baixa_main
    return baixa_main()


def executar_fluxo_completo(*, skip_extraction: bool = False) -> int:
    """Executa todas as etapas do pipeline em sequência."""
    inicio = time.time()
    log_session_start("Fluxo Completo")
    
    etapas = []
    
    if not skip_extraction:
        etapas.extend([
            ("Extração Email", extrair_email),
            ("Extração MAX", extrair_max),
        ])
    
    etapas.extend([
        ("Tratamento Tabelionato", tratar_tabelionato),
        ("Tratamento MAX", tratar_max),
        ("Batimento", executar_batimento),
        ("Baixa", executar_baixa),
    ])
    
    total = len(etapas)
    
    try:
        for i, (nome, funcao) in enumerate(etapas, 1):
            logger.info("[Passo %d/%d] %s", i, total, nome)
            try:
                funcao()
                logger.info("%s concluído com sucesso", nome)
            except Exception as exc:
                logger.error("Erro em %s: %s", nome, exc)
                log_session_end("Fluxo Completo", success=False)
                return 1
        
        duracao = time.time() - inicio
        print_section("FLUXO COMPLETO - SUCESSO", [
            f"Etapas executadas: {total}",
            f"Duração total: {format_duration(duracao)}",
        ])
        
        log_session_end("Fluxo Completo", success=True)
        return 0
        
    except Exception as exc:
        logger.exception("Erro no fluxo completo: %s", exc)
        log_session_end("Fluxo Completo", success=False)
        return 1


def _execute_pipeline(args: argparse.Namespace) -> None:
    """Executa o pipeline conforme os argumentos."""
    if args.command == "extract-email":
        extrair_email()
    elif args.command == "extract-max":
        extrair_max()
    elif args.command == "extract-all":
        extrair_email()
        extrair_max()
    elif args.command == "treat-tabelionato":
        tratar_tabelionato()
    elif args.command == "treat-max":
        tratar_max()
    elif args.command == "treat-all":
        tratar_tabelionato()
        tratar_max()
    elif args.command == "batimento":
        executar_batimento()
    elif args.command == "baixa":
        executar_baixa()
    elif args.command == "full":
        skip = getattr(args, "skip_extraction", False)
        result = executar_fluxo_completo(skip_extraction=skip)
        if result != 0:
            raise RuntimeError("Fluxo completo falhou")


def main(argv: list[str] | None = None) -> int:
    """Ponto de entrada principal com console runner."""
    parser = argparse.ArgumentParser(
        prog="python main.py",
        description="Pipeline Tabelionato - Extração, Tratamento, Batimento e Baixa"
    )

    # Argumento global para desabilitar espera
    parser.add_argument('--no-wait', action='store_true',
                       help='Nao aguarda antes de fechar o console')

    subparsers = parser.add_subparsers(dest="command", required=False)

    # Comandos individuais
    subparsers.add_parser("extract-email", help="Extrai dados via email")
    subparsers.add_parser("extract-max", help="Extrai dados do MAX (SQL Server)")
    subparsers.add_parser("extract-all", help="Executa todas as extrações")

    subparsers.add_parser("treat-tabelionato", help="Trata dados do Tabelionato")
    subparsers.add_parser("treat-max", help="Trata dados do MAX")
    subparsers.add_parser("treat-all", help="Executa todos os tratamentos")

    subparsers.add_parser("batimento", help="Executa batimento Tabelionato x MAX")
    subparsers.add_parser("baixa", help="Executa processamento de baixa")

    # Fluxo completo
    full = subparsers.add_parser("full", help="Executa fluxo completo")
    full.add_argument("--skip-extraction", action="store_true", help="Pula etapas de extração")

    args = parser.parse_args(argv)

    # Se nenhum comando, executar fluxo completo
    if args.command is None:
        args.command = "full"

    # Se --no-wait, executar diretamente sem console runner
    if args.no_wait:
        try:
            _execute_pipeline(args)
            return 0
        except KeyboardInterrupt:
            print("\n Execucao interrompida pelo usuario")
            return 1
        except Exception as exc:
            logger.exception("Erro: %s", exc)
            return 1
    else:
        # Executar com console runner (espera 5 minutos antes de fechar)
        return run_with_console(
            project_name="TABELIONATO",
            base_dir=BASE_DIR,
            pipeline_func=lambda: _execute_pipeline(args),
            wait_time=300,  # 5 minutos
        )


if __name__ == "__main__":
    sys.exit(main())
