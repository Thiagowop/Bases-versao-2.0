"""Utilitários auxiliares comuns para processadores.

Consolida funções de:
- anti_join.py (procv_vic_menos_max, procv_max_menos_vic, procv_left_minus_right)
- text.py (normalize_ascii_upper, digits_only)
- helpers genéricos (primeiro_valor, normalizar_data_string, etc.)
"""

from __future__ import annotations

import re
import unicodedata
from numbers import Number
from typing import Any, Iterable, Optional

import pandas as pd


# =============================================================================
# FUNÇÕES DE TEXTO (originalmente em text.py)
# =============================================================================

def normalize_ascii_upper(serie: pd.Series) -> pd.Series:
    """Retorna série normalizada para comparação insensitive.

    Remove acentos, converte para maiúsculas e aplica strip.
    """
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
    """Retorna linhas de df_left cujas chaves não estão em df_right.

    Implementa anti-join simples usando conjunto de chaves normalizadas para
    boa performance e legibilidade.
    """

    if col_left not in df_left.columns:
        raise ValueError(f"Coluna obrigatória ausente no LEFT: {col_left}")
    if col_right not in df_right.columns:
        raise ValueError(f"Coluna obrigatória ausente no RIGHT: {col_right}")

    right_keys: Iterable[str] = set(_normalize_series(df_right[col_right]).dropna())
    mask = ~_normalize_series(df_left[col_left]).isin(right_keys)
    return df_left.loc[mask].copy()


def procv_max_menos_vic(
    df_max: pd.DataFrame,
    df_vic: pd.DataFrame,
    col_max: str = "PARCELA",
    col_vic: str = "CHAVE",
) -> pd.DataFrame:
    """Retorna K_dev = K_max − K_vic (linhas de MAX não presentes em VIC)."""

    return procv_left_minus_right(df_max, df_vic, col_max, col_vic)


def procv_vic_menos_max(
    df_vic: pd.DataFrame,
    df_max: pd.DataFrame,
    col_vic: str = "CHAVE",
    col_max: str = "PARCELA",
) -> pd.DataFrame:
    """Retorna K_bat = K_vic − K_max (linhas de VIC não presentes em MAX)."""

    return procv_left_minus_right(df_vic, df_max, col_vic, col_max)


# =============================================================================
# FUNÇÕES AUXILIARES GENÉRICAS
# =============================================================================

def primeiro_valor(series: Optional[pd.Series]) -> Optional[Any]:
    """Retorna o primeiro valor válido (não nulo e não vazio) de uma Series.
    
    Args:
        series: Series do pandas para extrair o primeiro valor válido
        
    Returns:
        Optional[Any]: Primeiro valor válido encontrado ou None se não houver
    """
    if series is None:
        return None
    
    for valor in series:
        if pd.isna(valor):
            continue
        texto = str(valor).strip()
        if not texto or texto.lower() == "nan":
            continue
        return valor
    
    return None


def normalizar_data_string(valor: Any) -> Optional[str]:
    """Normaliza um valor para string de data no formato dd/mm/yyyy.
    
    Args:
        valor: Valor a ser normalizado (str, pd.Timestamp, ou outro tipo)
        
    Returns:
        Optional[str]: Data normalizada no formato dd/mm/yyyy ou None se inválida
    """
    if valor is None:
        return None
    
    if isinstance(valor, str):
        texto = valor.strip()
        if not texto or texto.lower() == "nan":
            return None
        dt = pd.to_datetime(texto, errors="coerce", dayfirst=True)
    elif isinstance(valor, pd.Timestamp):
        dt = valor
    else:
        dt = pd.to_datetime(valor, errors="coerce", dayfirst=True)
    
    if pd.isna(dt):
        return None
    
    return dt.strftime("%d/%m/%Y")


def extrair_data_referencia(df: pd.DataFrame, colunas_candidatas: list[str]) -> Optional[str]:
    """Extrai data de referência do DataFrame usando lista de colunas candidatas.
    
    Args:
        df: DataFrame para extrair a data
        colunas_candidatas: Lista de nomes de colunas para tentar extrair a data
        
    Returns:
        Optional[str]: Data de referência normalizada ou None se não encontrada
    """
    candidatos = []
    
    for coluna in colunas_candidatas:
        if coluna in df.columns:
            candidatos.append(primeiro_valor(df[coluna]))
    
    for candidato in candidatos:
        if candidato is not None:
            valor_normalizado = normalizar_data_string(candidato)
            if valor_normalizado:
                return valor_normalizado
    
    return None


def normalizar_decimal(valor: Any) -> Optional[float]:
    """Converte valores com diferentes separadores decimais em float."""
    if valor is None:
        return None

    if isinstance(valor, Number):
        if pd.isna(valor):
            return None
        try:
            return float(valor)
        except (TypeError, ValueError):
            return None

    if pd.isna(valor):
        return None

    texto = str(valor).strip()
    if not texto:
        return None

    if texto.lower() in {"nan", "none", "null"}:
        return None

    texto = (
        texto.replace("R$", "")
        .replace("\u00A0", "")
        .replace("\u202F", "")
        .replace(" ", "")
        .replace("\t", "")
        .replace("\r", "")
        .replace("\n", "")
    )

    if "," in texto and "." in texto:
        texto = texto.replace(".", "").replace(",", ".")
    elif "," in texto:
        texto = texto.replace(",", ".")
    elif texto.count(".") > 1:
        texto = texto.replace(".", "")

    texto = texto.replace("'", "")
    texto = re.sub(r"[^0-9\.\-]", "", texto)

    if not texto or texto in {".", "-", "-.", ".-", "--"}:
        return None

    try:
        return float(texto)
    except ValueError:
        return None


def formatar_valor_string(valor: Any) -> str:
    """Formata um valor como string, tratando casos especiais.
    
    Args:
        valor: Valor a ser formatado
        
    Returns:
        str: Valor formatado como string
    """
    if valor is None or (isinstance(valor, float) and pd.isna(valor)):
        return ""
    
    texto = str(valor).strip()
    return texto if texto.lower() != "nan" else ""


def extrair_telefone(valor: Any) -> str:
    """Extrai e formata número de telefone removendo caracteres não numéricos.
    
    Args:
        valor: Valor a ser processado
        
    Returns:
        String com apenas dígitos do telefone
    """
    if pd.isna(valor) or valor is None:
        return ""
    
    texto = str(valor).strip()
    if not texto or texto.lower() in ("nan", "none", "null"):
        return ""
    
    # Remove todos os caracteres não numéricos
    digitos = ''.join(filter(str.isdigit, texto))
    return digitos


def formatar_datas_serie(serie: pd.Series, formato: str = "%d/%m/%Y") -> pd.Series:
    """Formata uma série de datas para string no formato especificado.
    
    Args:
        serie: Série pandas com datas
        formato: Formato de saída da data (padrão: dd/mm/yyyy)
        
    Returns:
        Série com datas formatadas como string
    """
    valores = pd.to_datetime(serie, errors="coerce", dayfirst=True)
    formatted = valores.dt.strftime(formato)
    return formatted.fillna("")


# =============================================================================
# FUNÇÕES DE AGING (originalmente em aging.py)
# =============================================================================

from datetime import datetime
from typing import Set, Tuple


def filtrar_clientes_criticos(
    df: pd.DataFrame,
    col_cliente: str,
    col_vencimento: str,
    limite: int,
    data_referencia: datetime | None = None,
) -> Tuple[pd.DataFrame, Set[str]]:
    """Filtra clientes com aging acima do limite e retorna IDs removidos.

    Args:
        df: Dataset original.
        col_cliente: Nome da coluna com identificador do cliente.
        col_vencimento: Nome da coluna com data de vencimento.
        limite: Threshold (em dias) para considerar cliente crítico.
        data_referencia: Data de referência para cálculo de aging (default: now).

    Returns:
        Tuple[pd.DataFrame, Set[str]]: DataFrame filtrado e set de clientes removidos.
    """
    if col_cliente not in df.columns:
        raise ValueError("Coluna de cliente ausente para cálculo de aging")
    if col_vencimento not in df.columns:
        raise ValueError("Coluna de vencimento ausente para cálculo de aging")

    if df.empty:
        return df.copy(), set()

    ref = pd.Timestamp(data_referencia or datetime.now())

    df_work = df.copy()
    vencimentos = pd.to_datetime(df_work[col_vencimento], errors="coerce")
    invalid_mask = vencimentos.isna()

    clientes_invalidos = set(
        df_work.loc[invalid_mask, col_cliente].astype(str).str.strip()
    )
    df_work = df_work.loc[~invalid_mask].copy()

    if df_work.empty:
        return df_work, {c for c in clientes_invalidos if c}

    aging = (ref - vencimentos.loc[~invalid_mask]).dt.days.clip(lower=0)
    df_work["_AGING_POS"] = aging

    aging_por_cliente = df_work.groupby(col_cliente)["_AGING_POS"].max()
    clientes_criticos = set(aging_por_cliente[aging_por_cliente >= limite].index)

    df_filtrado = df_work[df_work[col_cliente].isin(clientes_criticos)].copy()
    df_filtrado.drop(columns=["_AGING_POS"], inplace=True, errors="ignore")

    clientes_remanescentes = set(
        df_filtrado[col_cliente].astype(str).str.strip()
    )
    clientes_originais = set(df[col_cliente].astype(str).str.strip())

    clientes_removidos = {
        c for c in clientes_originais if c and c not in clientes_remanescentes
    }.union({c for c in clientes_invalidos if c})

    return df_filtrado, clientes_removidos

__all__ = [
    # Funções de texto
    "normalize_ascii_upper",
    "digits_only",
    # Funções de anti-join
    "procv_left_minus_right",
    "procv_max_menos_vic",
    "procv_vic_menos_max",
    # Funções auxiliares
    "primeiro_valor",
    "normalizar_data_string",
    "extrair_data_referencia",
    "normalizar_decimal",
    "formatar_valor_string",
    "extrair_telefone",
    "formatar_datas_serie",
    # Funções de aging
    "filtrar_clientes_criticos",
    # Funções de log parsing
    "clean_extraction_line",
    "extract_extraction_value",
    "parse_extraction_summary",
    # Classe JudicialHelper
    "JudicialHelper",
]


# =============================================================================
# CLASSE JUDICIAL HELPER (consolidada de batimento, devolucao, baixa)
# =============================================================================

from logging import Logger
from pathlib import Path
from typing import Callable


class JudicialHelper:
    """Classe utilitária para gerenciar CPFs judiciais e divisão de carteiras.

    Consolida a lógica duplicada que existia em:
    - batimento.py: carregar_cpfs_judiciais()
    - devolucao.py: _carregar_cpfs_judiciais(), _mask_judicial()
    - baixa.py: _carregar_cpfs_judiciais(), _mask_judicial()
    """

    # Colunas padrão para buscar CPF no DataFrame
    CPF_COLUMNS = ("CPFCNPJ_CLIENTE", "CPF_CNPJ", "CPF/CNPJ", "CPF")

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
            file_reader: Função para ler arquivos CSV/ZIP (ex: file_manager.ler_csv_ou_zip)
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
        inputs_config = self.config.get("inputs", {})
        judicial_path_cfg = inputs_config.get("clientes_judiciais_path")

        if judicial_path_cfg:
            judicial_file = Path(judicial_path_cfg)
        else:
            judicial_dir = self._paths_config.get("input", {}).get("judicial")
            if judicial_dir:
                judicial_file = Path(judicial_dir) / "ClientesJudiciais.zip"
            else:
                judicial_file = Path("data/input/judicial/ClientesJudiciais.zip")

        if not judicial_file.is_absolute():
            judicial_file = Path.cwd() / judicial_file

        if not judicial_file.exists():
            self.logger.warning(
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
                import zipfile
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
            cpf_columns = [col for col in df_judicial.columns if "CPF" in str(col).upper()]
            if not cpf_columns:
                self.logger.warning("Nenhuma coluna de CPF encontrada no arquivo judicial")
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

        Verifica primeiro colunas IS_JUDICIAL e TIPO_FLUXO, depois usa CPFs.

        Args:
            df: DataFrame com registros a classificar

        Returns:
            Series booleana: True = judicial, False = extrajudicial
        """
        # Verificar coluna IS_JUDICIAL
        if "IS_JUDICIAL" in df.columns:
            serie = df["IS_JUDICIAL"].astype(str).str.upper().str.strip()
            return serie.isin({"1", "SIM", "TRUE", "JUDICIAL"})

        # Verificar coluna TIPO_FLUXO
        if "TIPO_FLUXO" in df.columns:
            serie = df["TIPO_FLUXO"].astype(str).str.upper().str.strip()
            return serie.eq("JUDICIAL")

        # Usar CPFs judiciais
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

        serie = digits_only(df[cpf_col])
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
# FUNÇÕES DE LOG PARSING (originalmente em log_parser.py)
# =============================================================================

_TAG_PREFIX_RE = re.compile(r"^\[[^\]]+\]\s*")

_SUMMARY_FIELDS = [
    ("anexos_encontrados", "📥", "Anexos encontrados"),
    ("anexos_baixados", "📥", "Anexos baixados"),
    ("registros", "📊", "Total de registros extraídos"),
    ("arquivo", "📁", "Arquivo salvo em"),
    ("tempo", "⏱️", "Tempo de execução"),
    ("email_data", "📅", "Data/hora do e-mail"),
]


def clean_extraction_line(line: str) -> str:
    """Remove prefixos e espaços extras de uma linha de log de extração."""
    return _TAG_PREFIX_RE.sub("", line).strip()


def extract_extraction_value(line: str) -> str:
    """Extrai valor após ':' de uma linha."""
    if ":" not in line:
        return ""
    return line.split(":", 1)[1].strip()


def parse_extraction_summary(stdout: str) -> tuple[dict[str, str], list]:
    """Parseia saída de script de extração e retorna resumo + avisos."""
    resumo: dict[str, str] = {}
    avisos: list = []

    for linha in stdout.splitlines():
        trecho = linha.strip()
        if not trecho:
            continue

        limpa = clean_extraction_line(trecho)
        if not limpa:
            continue

        if all(char == "=" for char in limpa):
            continue

        texto_minusculo = limpa.lower()

        if "[aviso]" in linha.lower():
            avisos.append(limpa)

        if "anexos encontrados" in texto_minusculo:
            resumo["anexos_encontrados"] = extract_extraction_value(limpa)
            continue

        if "anexos baixados" in texto_minusculo:
            resumo["anexos_baixados"] = extract_extraction_value(limpa)
            continue

        if any(
            palavra in texto_minusculo
            for palavra in ("registros extra", "registros encontrados", "registros únicos")
        ):
            resumo["registros"] = extract_extraction_value(limpa)
            continue

        if any(
            palavra in texto_minusculo
            for palavra in ("arquivo salvo", "arquivo gerado", "caminho")
        ):
            valor = extract_extraction_value(limpa)
            if valor:
                resumo["arquivo"] = valor
            continue

        if "tempo de execução" in texto_minusculo:
            resumo["tempo"] = extract_extraction_value(limpa)
            continue

        if "data/hora" in texto_minusculo:
            resumo["email_data"] = extract_extraction_value(limpa)

    return resumo, avisos
