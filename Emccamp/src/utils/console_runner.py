"""Console Runner - Módulo padronizado para execução com logs e espera.

Fornece:
- Execução segura com tratamento de erros
- Logs claros para o usuário (console + arquivo)
- Espera de 5 minutos antes de fechar (configurável)
- Mensagens de erro amigáveis e específicas
"""

from __future__ import annotations

import logging
import os
import sys
import time
import traceback
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, Optional

# Detectar plataforma para keyboard input
_IS_WINDOWS = os.name == 'nt'
if _IS_WINDOWS:
    import msvcrt
else:
    import select

# Tempo de espera padrão em segundos (5 minutos)
DEFAULT_WAIT_TIME = 300


class ErrorCategory(Enum):
    """Categorias de erro para mensagens amigáveis."""
    CONNECTION = "connection"
    AUTHENTICATION = "authentication"
    FILE_NOT_FOUND = "file_not_found"
    PERMISSION = "permission"
    DATA_VALIDATION = "data_validation"
    API = "api"
    DATABASE = "database"
    EMAIL = "email"
    UNKNOWN = "unknown"


# Mapeamento de padrões de erro para categorias e mensagens
ERROR_PATTERNS = {
    # Erros de conexão
    ("connection refused", "connect timeout", "network unreachable", "no route to host"): {
        "category": ErrorCategory.CONNECTION,
        "message": "Falha na conexao de rede",
        "suggestion": "Verifique se a VPN esta conectada e se o servidor esta acessivel",
    },
    # Erros de autenticação/credenciais
    ("invalid credentials", "authentication failed", "login failed", "unauthorized", "401", "invalid password", "incorrect password"): {
        "category": ErrorCategory.AUTHENTICATION,
        "message": "Falha na autenticacao",
        "suggestion": "Verifique as credenciais no arquivo .env (usuario/senha)",
    },
    # Erros de API
    ("api error", "api request failed", "rate limit", "429", "500", "503", "bad gateway"): {
        "category": ErrorCategory.API,
        "message": "Erro na API externa",
        "suggestion": "A API pode estar temporariamente indisponivel. Tente novamente em alguns minutos",
    },
    # Erros de banco de dados
    ("sql server", "database error", "query failed", "connection to database", "pyodbc", "cannot open database"): {
        "category": ErrorCategory.DATABASE,
        "message": "Erro no banco de dados",
        "suggestion": "Verifique a conexao com o SQL Server e as credenciais no .env",
    },
    # Erros de email
    ("imap", "smtp", "mail server", "authentication failed", "gmail", "email"): {
        "category": ErrorCategory.EMAIL,
        "message": "Erro no servidor de email",
        "suggestion": "Verifique as credenciais de email e se a senha de app esta correta",
    },
    # Erros de arquivo
    ("file not found", "no such file", "path does not exist", "filenotfounderror"): {
        "category": ErrorCategory.FILE_NOT_FOUND,
        "message": "Arquivo ou diretorio nao encontrado",
        "suggestion": "Verifique se os arquivos de entrada existem em data/input/",
    },
    # Erros de permissão
    ("permission denied", "access denied", "permissionerror"): {
        "category": ErrorCategory.PERMISSION,
        "message": "Permissao negada",
        "suggestion": "Verifique as permissoes de acesso aos arquivos/diretorios",
    },
}


def _kbhit() -> bool:
    """Verifica se uma tecla foi pressionada (cross-platform)."""
    if _IS_WINDOWS:
        return msvcrt.kbhit()
    else:
        # Unix/Linux/Mac: usar select com stdin
        dr, _, _ = select.select([sys.stdin], [], [], 0)
        return bool(dr)


def _getch() -> str:
    """Lê um caractere do teclado (cross-platform)."""
    if _IS_WINDOWS:
        return msvcrt.getch().decode('utf-8', errors='ignore')
    else:
        return sys.stdin.read(1)


class ConsoleRunner:
    """Executor de pipeline com logging e tratamento de erros."""

    def __init__(
        self,
        project_name: str,
        base_dir: Path,
        wait_time: int = DEFAULT_WAIT_TIME,
    ):
        """
        Inicializa o runner.

        Args:
            project_name: Nome do projeto (ex: "VIC", "EMCCAMP", "TABELIONATO")
            base_dir: Diretório raiz do projeto
            wait_time: Tempo de espera em segundos antes de fechar (padrão: 300)
        """
        self.project_name = project_name.upper()
        self.base_dir = Path(base_dir)
        self.wait_time = wait_time
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.success = False
        self.error_info: Optional[Dict[str, Any]] = None

        # Configurar diretório de logs
        self.logs_dir = self.base_dir / "data" / "logs"
        self.logs_dir.mkdir(parents=True, exist_ok=True)

        # Arquivo de log da sessão
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.session_log_file = self.logs_dir / f"sessao_{timestamp}.log"

        # Configurar logger
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """Configura logger com handlers para console e arquivo."""
        logger = logging.getLogger(f"console_runner_{self.project_name}")
        logger.setLevel(logging.DEBUG)
        logger.handlers.clear()

        # Formato para arquivo (detalhado)
        file_format = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        # Formato para console (limpo)
        console_format = logging.Formatter("%(message)s")

        # Handler para arquivo
        file_handler = logging.FileHandler(
            self.session_log_file, mode="w", encoding="utf-8"
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(file_format)
        logger.addHandler(file_handler)

        # Handler para console
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(console_format)
        logger.addHandler(console_handler)

        return logger

    def _classify_error(self, error: Exception) -> Dict[str, Any]:
        """Classifica o erro e retorna informações amigáveis."""
        error_str = str(error).lower()
        error_type = type(error).__name__.lower()
        full_error = f"{error_type}: {error_str}"

        for patterns, info in ERROR_PATTERNS.items():
            for pattern in patterns:
                if pattern in full_error:
                    return {
                        "category": info["category"],
                        "message": info["message"],
                        "suggestion": info["suggestion"],
                        "original_error": str(error),
                        "error_type": type(error).__name__,
                    }

        # Erro não categorizado
        return {
            "category": ErrorCategory.UNKNOWN,
            "message": "Erro inesperado",
            "suggestion": "Consulte o arquivo de log para mais detalhes",
            "original_error": str(error),
            "error_type": type(error).__name__,
        }

    def _print_header(self) -> None:
        """Imprime cabeçalho da execução."""
        separator = "=" * 60
        self.logger.info("")
        self.logger.info(separator)
        self.logger.info(f"   PIPELINE {self.project_name}")
        self.logger.info(f"   Iniciado em: {self.start_time.strftime('%d/%m/%Y %H:%M:%S')}")
        self.logger.info(separator)
        self.logger.info("")

    def _print_success(self, duration: float) -> None:
        """Imprime mensagem de sucesso."""
        separator = "=" * 60
        self.logger.info("")
        self.logger.info(separator)
        self.logger.info("   EXECUCAO CONCLUIDA COM SUCESSO!")
        self.logger.info(separator)
        self.logger.info(f"   Projeto: {self.project_name}")
        self.logger.info(f"   Duracao: {self._format_duration(duration)}")
        self.logger.info(f"   Log: {self.session_log_file}")
        self.logger.info(separator)
        self.logger.info("")

    def _print_error(self, duration: float) -> None:
        """Imprime mensagem de erro formatada."""
        separator = "=" * 60
        error_sep = "-" * 60

        self.logger.info("")
        self.logger.error(separator)
        self.logger.error("   EXECUCAO FINALIZADA COM ERRO!")
        self.logger.error(separator)
        self.logger.error("")
        self.logger.error(error_sep)
        self.logger.error(f"   TIPO: {self.error_info['message']}")
        self.logger.error(error_sep)
        self.logger.error("")
        self.logger.error(f"   Erro: {self.error_info['original_error'][:200]}")
        self.logger.error("")
        self.logger.error(f"   SUGESTAO: {self.error_info['suggestion']}")
        self.logger.error("")
        self.logger.error(error_sep)
        self.logger.error(f"   Duracao: {self._format_duration(duration)}")
        self.logger.error(f"   Log completo: {self.session_log_file}")
        self.logger.error(separator)
        self.logger.error("")

    def _format_duration(self, seconds: float) -> str:
        """Formata duração em formato legível."""
        if seconds < 60:
            return f"{seconds:.1f} segundos"
        elif seconds < 3600:
            minutes = int(seconds // 60)
            secs = int(seconds % 60)
            return f"{minutes}m {secs}s"
        else:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            return f"{hours}h {minutes}m"

    def _wait_before_close(self) -> None:
        """Aguarda antes de fechar o console (qualquer tecla ou timeout)."""
        self.logger.info("")
        self.logger.info("-" * 60)
        self.logger.info(f"   Console fechara automaticamente em {self.wait_time // 60} minutos")
        self.logger.info("   Pressione qualquer tecla para fechar imediatamente")
        self.logger.info("-" * 60)
        self.logger.info("")

        try:
            remaining = self.wait_time
            while remaining > 0:
                # Verificar se alguma tecla foi pressionada
                if _kbhit():
                    _getch()  # Consumir a tecla
                    print("\n")
                    self.logger.info("Fechamento confirmado pelo usuario.")
                    return

                mins = remaining // 60
                secs = remaining % 60
                # Atualizar a cada 30 segundos para não poluir muito
                if remaining % 30 == 0 or remaining <= 10:
                    print(f"\r   Tempo restante: {mins:02d}:{secs:02d}   ", end="", flush=True)
                time.sleep(1)
                remaining -= 1

            print("\n")
            self.logger.info("Fechando automaticamente...")

        except KeyboardInterrupt:
            print("\n")
            self.logger.info("Fechamento antecipado pelo usuario.")

    def run(
        self,
        pipeline_func: Callable[[], Any],
        show_traceback: bool = False,
    ) -> int:
        """
        Executa o pipeline com tratamento de erros e espera.

        Args:
            pipeline_func: Função do pipeline a executar
            show_traceback: Se True, mostra traceback completo no console

        Returns:
            0 se sucesso, 1 se erro
        """
        self.start_time = datetime.now()

        # Header do log
        self._print_header()

        try:
            # Executar pipeline
            pipeline_func()

            self.success = True
            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()

            self._print_success(duration)

        except KeyboardInterrupt:
            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()

            self.logger.warning("")
            self.logger.warning("=" * 60)
            self.logger.warning("   EXECUCAO INTERROMPIDA PELO USUARIO")
            self.logger.warning("=" * 60)
            self.logger.warning(f"   Duracao: {self._format_duration(duration)}")
            self.logger.warning("")

            self.success = False

        except Exception as e:
            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()

            # Classificar e registrar erro
            self.error_info = self._classify_error(e)

            # Log detalhado para arquivo
            self.logger.debug("Traceback completo:")
            self.logger.debug(traceback.format_exc())

            if show_traceback:
                self.logger.error(traceback.format_exc())

            self._print_error(duration)
            self.success = False

        # Aguardar antes de fechar
        self._wait_before_close()

        return 0 if self.success else 1


def run_with_console(
    project_name: str,
    base_dir: Path,
    pipeline_func: Callable[[], Any],
    wait_time: int = DEFAULT_WAIT_TIME,
    show_traceback: bool = False,
) -> int:
    """
    Função de conveniência para executar pipeline com console runner.

    Args:
        project_name: Nome do projeto
        base_dir: Diretório raiz do projeto
        pipeline_func: Função do pipeline a executar
        wait_time: Tempo de espera em segundos (padrão: 300)
        show_traceback: Se True, mostra traceback completo

    Returns:
        Código de saída (0 = sucesso, 1 = erro)
    """
    runner = ConsoleRunner(
        project_name=project_name,
        base_dir=base_dir,
        wait_time=wait_time,
    )
    return runner.run(pipeline_func, show_traceback=show_traceback)
