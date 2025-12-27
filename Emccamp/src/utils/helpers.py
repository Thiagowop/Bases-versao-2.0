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
    # Classe JudicialHelper
    "JudicialHelper",
]


# =============================================================================
# CLASSE JUDICIAL HELPER (consolidada de batimento e devolucao)
# =============================================================================

from logging import Logger
from pathlib import Path
from typing import Callable, Dict, Set, Tuple
import zipfile


class JudicialHelper:
    """Classe utilitária para gerenciar CPFs judiciais e divisão de carteiras.

    Consolida a lógica duplicada que existia em:
    - batimento.py: _load_judicial_cpfs(), _split_portfolios()
    - devolucao.py: _carregar_cpfs_judiciais(), _dividir_carteiras()
    """

    # Colunas padrão para buscar CPF no DataFrame
    CPF_COLUMNS = ("CPFCNPJ_CLIENTE", "CPFCNPJ CLIENTE", "CPF_CNPJ", "CPF/CNPJ", "CPF")

    def __init__(
        self,
        config: Dict[str, Any],
        logger: Logger,
        file_reader: Optional[Callable[[Path], pd.DataFrame]] = None,
    ):
        """Inicializa o JudicialHelper.

        Args:
            config: Dicionário de configurações do projeto
            logger: Logger para registro de eventos
            file_reader: Função para ler arquivos CSV/ZIP
        """
        self.config = config
        self.logger = logger
        self._file_reader = file_reader
        self._judicial_cpfs: Optional[Set[str]] = None
        self._paths_config = config.get("paths", {})

    @property
    def judicial_cpfs(self) -> Set[str]:
        """Retorna o conjunto de CPFs judiciais, carregando se necessário."""
        if self._judicial_cpfs is None:
            self._carregar_cpfs_judiciais()
        return self._judicial_cpfs or set()

    def _carregar_cpfs_judiciais(self) -> None:
        """Carrega CPFs dos clientes judiciais a partir de CSV/ZIP."""
        if self._judicial_cpfs is not None:
            return

        # Determinar caminho do arquivo
        judicial_dir = self._paths_config.get("input", {}).get("judicial")
        if judicial_dir:
            judicial_file = Path(judicial_dir) / "ClientesJudiciais.zip"
        else:
            judicial_file = Path("data/input/judicial/ClientesJudiciais.zip")

        if not judicial_file.is_absolute():
            judicial_file = Path.cwd() / judicial_file

        if not judicial_file.exists():
            self.logger.info(
                "Arquivo de clientes judiciais não encontrado: %s. "
                "Todos os registros serão classificados como EXTRAJUDICIAL",
                judicial_file,
            )
            self._judicial_cpfs = set()
            return

        try:
            self.logger.info(f"Carregando clientes judiciais: {judicial_file}")

            if self._file_reader:
                df_judicial = self._file_reader(judicial_file)
            else:
                # Fallback: leitura básica
                if judicial_file.suffix.lower() == ".zip":
                    with zipfile.ZipFile(judicial_file, "r") as zf:
                        csv_files = [n for n in zf.namelist() if n.lower().endswith(".csv")]
                        if csv_files:
                            with zf.open(csv_files[0]) as fh:
                                df_judicial = pd.read_csv(fh, dtype=str, sep=";", encoding="utf-8-sig")
                        else:
                            self._judicial_cpfs = set()
                            return
                else:
                    df_judicial = pd.read_csv(judicial_file, dtype=str, sep=";", encoding="utf-8-sig")

            # Encontrar coluna de CPF
            cpf_columns = [
                col for col in df_judicial.columns
                if "CPF" in str(col).upper() or "CNPJ" in str(col).upper()
            ]
            if not cpf_columns:
                self.logger.warning("Nenhuma coluna de CPF/CNPJ encontrada no arquivo judicial")
                self._judicial_cpfs = set()
                return

            # Normalizar e validar CPFs (11 ou 14 dígitos)
            cpfs_norm = digits_only(df_judicial[cpf_columns[0]].dropna())
            cpfs_valid = cpfs_norm[cpfs_norm.str.len().isin({11, 14})]
            self._judicial_cpfs = set(cpfs_valid.tolist())

            self.logger.info(f"CPFs judiciais carregados: {len(self._judicial_cpfs):,}")

        except Exception as exc:
            self.logger.warning("Falha ao carregar CPFs judiciais: %s", exc)
            self._judicial_cpfs = set()

    def mask_judicial(self, df: pd.DataFrame) -> pd.Series:
        """Retorna máscara booleana indicando quais registros são judiciais.

        Args:
            df: DataFrame com registros a classificar

        Returns:
            Series booleana: True = judicial, False = extrajudicial
        """
        if not self.judicial_cpfs:
            return pd.Series([False] * len(df), index=df.index)

        # Buscar coluna de CPF
        cpf_col = None
        for col in self.CPF_COLUMNS:
            if col in df.columns:
                cpf_col = col
                break

        if cpf_col is None:
            return pd.Series([False] * len(df), index=df.index)

        serie = digits_only(df[cpf_col].fillna(""))
        return serie.isin(self.judicial_cpfs)

    def dividir_carteiras(
        self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Divide DataFrame em carteiras judicial e extrajudicial.

        Args:
            df: DataFrame com registros a dividir

        Returns:
            Tuple (df_judicial, df_extrajudicial)
        """
        if df.empty:
            return df.copy(), df.copy()

        mask = self.mask_judicial(df)
        df_judicial = df.loc[mask].copy()
        df_extrajudicial = df.loc[~mask].copy()

        return df_judicial, df_extrajudicial


# =============================================================================
# FUNÇÕES UTILITÁRIAS CENTRALIZADAS
# =============================================================================

# Constantes para filtros de status
VALID_OPEN_STATUSES = frozenset({'aberto', 'em aberto', 'vencido', 'a vencer'})


def generate_timestamp() -> str:
    """Gera timestamp padronizado do projeto (YYYYMMDD_HHMMSS)."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def limpar_arquivos_padrao(
    diretorio: 'Path',
    padrao: str,
    logger: Any = None,
) -> int:
    """Remove arquivos conforme padrão glob, retorna quantidade removida.

    Args:
        diretorio: Diretório onde buscar
        padrao: Padrão glob (ex: "*.zip", "tratado_*.csv")
        logger: Logger opcional para registrar operações

    Returns:
        Quantidade de arquivos removidos
    """
    from pathlib import Path
    diretorio = Path(diretorio)
    if not diretorio.exists():
        return 0

    removidos = 0
    for arquivo in diretorio.glob(padrao):
        try:
            arquivo.unlink()
            removidos += 1
            if logger:
                logger.debug(f"Arquivo removido: {arquivo}")
        except OSError as exc:
            if logger:
                logger.warning(f"Erro ao remover {arquivo}: {exc}")

    return removidos


def obter_arquivo_mais_recente(
    diretorio: 'Path',
    padrao: str = "*",
) -> Optional['Path']:
    """Retorna o arquivo mais recente no diretório conforme padrão.

    Args:
        diretorio: Diretório onde buscar
        padrao: Padrão glob (ex: "*.zip")

    Returns:
        Path do arquivo mais recente ou None se não encontrar
    """
    from pathlib import Path
    diretorio = Path(diretorio)
    if not diretorio.exists():
        return None

    arquivos = list(diretorio.glob(padrao))
    if not arquivos:
        return None

    return max(arquivos, key=lambda x: x.stat().st_mtime)


def filtrar_status_aberto(
    df: 'pd.DataFrame',
    coluna: str = 'STATUS_TITULO',
) -> 'pd.DataFrame':
    """Filtra registros com status em aberto.

    Args:
        df: DataFrame a filtrar
        coluna: Nome da coluna de status

    Returns:
        DataFrame filtrado apenas com registros em aberto
    """
    import pandas as pd
    if coluna not in df.columns:
        return df.copy()

    status_norm = df[coluna].astype(str).str.strip().str.upper()
    mask = status_norm.isin({s.upper() for s in VALID_OPEN_STATUSES})
    return df[mask].copy()
