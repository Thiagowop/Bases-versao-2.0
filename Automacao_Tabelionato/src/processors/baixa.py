#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Processador de baixa do Tabelionato.

Identifica protocolos que estão na MAX tratada mas não no Tabelionato tratado,
enriquece com dados de custas e gera layout final de recebimento.
"""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional
import os

import pandas as pd

from src.core.base_processor import BaseProcessor
from src.utils.helpers import format_duration, format_int, print_section, suppress_console_info
from src.utils.logger_config import (
    get_logger,
    log_metrics,
    log_session_end,
    log_session_start,
    log_validation_presence,
    log_validation_result,
)
from src.utils.validacao_resultados import (
    localizar_chaves_ausentes,
    localizar_chaves_presentes,
    resumir_amostras,
)

# Configuracao de separador decimal do CSV final (padrao brasileiro: ',')
DECIMAL_SEP = os.getenv("CSV_DECIMAL_SEPARATOR", ",")


@dataclass
class ResultadoBaixa:
    """Representa o resultado da execução do processo de baixa."""

    status: str
    mensagem: str = ""
    arquivo_final: Optional[str] = None
    arquivo_checagem: Optional[str] = None
    total_exportados: int = 0
    total_nao_exportados: int = 0
    duracao: float = 0.0


class BaixaProcessor(BaseProcessor):
    """Processador de baixa Tabelionato - herda de BaseProcessor."""

    def __init__(self, config: Dict[str, Any] = None):
        """Inicializa o processador de baixa.

        Args:
            config: Configurações do projeto (opcional)
        """
        # Configuração básica
        self.base_dir = Path(__file__).parent.parent.parent
        self.output_dir = self.base_dir / "data" / "output"
        self.baixa_dir = self.output_dir / "baixa"
        self.log_dir = self.base_dir / "data" / "logs"

        # Logger
        self.logger = get_logger("baixa")
        suppress_console_info(self.logger)

        # Configurações
        self.encoding = 'utf-8'
        self.separator = ';'

        # Inicializar BaseProcessor (se config fornecido)
        if config:
            super().__init__(config, self.logger)
        else:
            self.config = {}
            self.paths_config = {}

        # Criar diretórios
        self.baixa_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)

    def _limpar_arquivos_antigos(self, diretorio: Path, padrao: str) -> None:
        """Remove arquivos antigos no diretório informado conforme padrão."""
        if not diretorio.exists():
            return

        for arquivo in diretorio.glob(padrao):
            try:
                arquivo.unlink()
                self.logger.debug("Arquivo antigo removido: %s", arquivo)
            except OSError as exc:
                self.logger.warning("Não foi possível remover arquivo antigo %s: %s", arquivo, exc)

    def _carregar_base_custas(self) -> pd.DataFrame:
        """Carrega a base de custas do arquivo ZIP original."""
        custas_input_dir = self.base_dir / "data" / "input" / "tabelionato custas"

        if not custas_input_dir.exists():
            raise FileNotFoundError(
                f"Diretório de custas não encontrado: {custas_input_dir}. "
                "Gere ou informe o arquivo de custas antes de prosseguir."
            )

        arquivos_custas = sorted(custas_input_dir.glob("*.zip"))

        if not arquivos_custas:
            raise FileNotFoundError(
                f"Nenhum arquivo de custas encontrado em {custas_input_dir}. "
                "A baixa depende desta base."
            )

        # Usar o arquivo mais recente
        arquivo_custas = max(arquivos_custas, key=lambda x: x.stat().st_mtime)
        self.logger.info(f"Carregando custas de: {arquivo_custas}")

        df = self.carregar_arquivo(arquivo_custas, self.encoding, self.separator)
        self.logger.info("Base de custas carregada: %s registros", len(df))

        # Criar coluna CHAVE_STR para compatibilidade
        serie_protocolo = df.get('Protocolo', pd.Series(index=df.index, dtype='object'))
        df['Protocolo_Tratado'] = serie_protocolo.astype(str).str.strip()

        if 'Valor Total Pago' not in df.columns:
            df['Valor Total Pago'] = 0.0
        else:
            df['Valor Total Pago'] = pd.to_numeric(
                df['Valor Total Pago'], errors='coerce'
            ).fillna(0.0)

        return df

    def _filtrar_max_status_aberto(self, df_max: pd.DataFrame) -> pd.DataFrame:
        """Filtra registros com status em aberto na base MAX."""
        if 'STATUS_TITULO' not in df_max.columns:
            self.logger.warning("Coluna STATUS_TITULO não encontrada, retornando todos os registros")
            return df_max.copy()

        registros_antes = len(df_max)
        status_normalizado = (
            df_max['STATUS_TITULO']
            .astype(str)
            .str.strip()
            .str.lower()
        )
        status_validos = {'aberto', 'em aberto', 'a', '0'}
        mask_aberto = status_normalizado.isin(status_validos)
        df_filtrado = df_max[mask_aberto].copy()
        registros_depois = len(df_filtrado)

        self.logger.info(
            "Filtro status aberto MAX: %s → %s registros",
            registros_antes,
            registros_depois,
        )
        if registros_depois == 0:
            self.logger.warning("Nenhum registro com status considerado como aberto na base MAX.")
        return df_filtrado

    def _identificar_diferenca_max_tabelionato(
        self, df_max: pd.DataFrame, df_tabelionato: pd.DataFrame
    ) -> pd.DataFrame:
        """Identifica protocolos que estão na MAX mas não estão no Tabelionato."""
        chaves_max = set(df_max['CHAVE'].dropna())
        chaves_tabelionato = set(df_tabelionato['CHAVE'].dropna())

        # Chaves que estão na MAX mas não no Tabelionato
        chaves_diferenca = chaves_max - chaves_tabelionato

        self.logger.info("Chaves na MAX: %s", len(chaves_max))
        self.logger.info("Chaves no Tabelionato: %s", len(chaves_tabelionato))
        self.logger.info("Chaves apenas na MAX: %s", len(chaves_diferenca))

        # Filtrar DataFrame da MAX
        df_resultado = df_max[df_max['CHAVE'].isin(chaves_diferenca)].copy()

        self.logger.info("Registros finais após diferença: %s", len(df_resultado))
        return df_resultado

    def _enriquecer_com_custas(
        self, df_diferenca: pd.DataFrame, df_custas: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Enriquece os dados da diferença com informações de custas."""
        registros_antes = len(df_diferenca)

        df_trabalho = df_diferenca.copy()
        df_trabalho['CHAVE_STR'] = df_trabalho['CHAVE'].astype(str)

        df_custas_trabalho = df_custas.copy()
        df_custas_trabalho['CHAVE_STR'] = df_custas_trabalho['Protocolo_Tratado'].astype(str)

        # Merge LEFT para manter todos os registros da diferença
        df_merged = df_trabalho.merge(
            df_custas_trabalho[['CHAVE_STR', 'Valor Total Pago']],
            on='CHAVE_STR',
            how='left',
        )

        # Separar registros com match e sem match
        df_enriquecido = df_merged[df_merged['Valor Total Pago'].notna()].copy()
        df_checagem = df_merged[df_merged['Valor Total Pago'].isna()].copy()

        if not df_enriquecido.empty:
            df_enriquecido['Valor Total Pago'] = df_enriquecido['Valor Total Pago'].fillna(0)

        if not df_checagem.empty:
            df_checagem = df_checagem.copy()
            df_checagem['MOTIVO_NAO_EXPORTADO'] = 'Sem match na base custas'

        # Remover coluna auxiliar
        for dataset in (df_enriquecido, df_checagem):
            if 'CHAVE_STR' in dataset.columns:
                dataset.drop(columns=['CHAVE_STR'], inplace=True)

        registros_exportados = len(df_enriquecido)
        registros_nao_exportados = len(df_checagem)

        self.logger.info(
            "Comparação de protocolos: %s → %s registros para baixa",
            registros_antes,
            registros_exportados,
        )
        self.logger.info("Registros sem match na base custas: %s", registros_nao_exportados)

        return df_enriquecido, df_checagem

    def _gerar_layout_final(self, df_enriquecido: pd.DataFrame, data_pagamento: str) -> pd.DataFrame:
        """Gera o layout final de recebimento conforme especificação."""
        df_final = pd.DataFrame()

        df_final['NOME CLIENTE'] = df_enriquecido['NOME_RAZAO_SOCIAL']
        df_final['CPF/CNPJ CLIENTE'] = df_enriquecido['CPFCNPJ_CLIENTE']
        df_final['CNPJ CREDOR'] = df_enriquecido['CNPJ_CREDOR']
        df_final['NUMERO DOC'] = df_enriquecido['CHAVE']
        df_final['VALOR DA PARCELA'] = df_enriquecido['VALOR']
        df_final['DT. VENCIMENTO'] = df_enriquecido['VENCIMENTO']
        df_final['STATUS ACORDO'] = 2

        if not data_pagamento:
            raise ValueError("DT. PAGAMENTO ausente: 'data_pagamento' não informado")
        df_final['DT. PAGAMENTO'] = data_pagamento

        df_final['VALOR RECEBIDO'] = df_enriquecido['Valor Total Pago']

        # Formatar colunas de valor
        def _fmt(v: float) -> str:
            return "" if pd.isna(v) else ("%.2f" % v).replace(".", DECIMAL_SEP)

        def _to_numeric_brazil(s: pd.Series) -> pd.Series:
            s = s.astype(str).str.replace('R$', '', regex=False).str.replace(' ', '', regex=False)
            mask_comma = s.str.contains(',')
            s_br = s.copy()
            s_br[mask_comma] = s_br[mask_comma].str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
            s_br[~mask_comma] = s_br[~mask_comma].str.replace(',', '.', regex=False)
            return pd.to_numeric(s_br, errors='coerce')

        for col in ['VALOR DA PARCELA', 'VALOR RECEBIDO']:
            if col in df_final.columns:
                df_final[col] = _to_numeric_brazil(df_final[col]).map(_fmt)

        self.logger.info("Layout final gerado: %s registros", len(df_final))
        return df_final

    def _salvar_checagem(self, df_checagem: pd.DataFrame) -> None:
        """Registra informações sobre registros não exportados."""
        self._limpar_arquivos_antigos(self.baixa_dir, "checagem_nao_exportados_*")

        total_pendentes = len(df_checagem)
        if total_pendentes == 0:
            self.logger.info("Nenhum registro pendente para checagem.")
        else:
            self.logger.info(
                "Registros não exportados mantidos apenas em memória: %s",
                total_pendentes,
            )

    def _salvar_resultado(self, df_final: pd.DataFrame) -> str:
        """Salva o resultado final da baixa em formato ZIP."""
        self.baixa_dir.mkdir(parents=True, exist_ok=True)
        self._limpar_arquivos_antigos(self.baixa_dir, "baixa_tabelionato_*.zip")

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        caminho_zip = self.exportar_zip(
            df_final,
            self.baixa_dir,
            "baixa_tabelionato",
            timestamp,
            self.encoding,
            self.separator
        )

        self.logger.info("Resultado da baixa salvo: %s", caminho_zip)
        return str(caminho_zip)

    def processar(self, *args, **kwargs) -> Dict[str, Any]:
        """Executa o processo completo de baixa.

        Returns:
            Dicionário com estatísticas e resultado do processamento
        """
        resultado = self._executar_processo_baixa()

        return {
            'status': resultado.status,
            'mensagem': resultado.mensagem,
            'arquivo_final': resultado.arquivo_final,
            'total_exportados': resultado.total_exportados,
            'total_nao_exportados': resultado.total_nao_exportados,
            'duracao': resultado.duracao,
        }

    def _executar_processo_baixa(self) -> ResultadoBaixa:
        """Executa o processo completo de baixa."""
        log_session_start("Baixa Tabelionato")
        inicio = datetime.now()
        sucesso = False

        try:
            # 1. Carregar bases tratadas
            self.logger.info("1. Carregando bases tratadas...")

            # MAX tratada
            caminho_max = self.output_dir / "max_tratada" / "max_tratada.zip"
            if not caminho_max.exists():
                raise FileNotFoundError(f"Base MAX tratada não encontrada: {caminho_max}")
            df_max = self.carregar_arquivo(caminho_max, self.encoding, self.separator)

            # Tabelionato tratado
            caminho_tabelionato = self.output_dir / "tabelionato_tratada" / "tabelionato_tratado.zip"
            if not caminho_tabelionato.exists():
                raise FileNotFoundError(f"Base Tabelionato tratada não encontrada: {caminho_tabelionato}")
            df_tabelionato = self.carregar_arquivo(caminho_tabelionato, self.encoding, self.separator)

            # Obter DataExtracao
            if 'DataExtracao' not in df_tabelionato.columns:
                raise KeyError("Base Tabelionato tratada sem 'DataExtracao'.")
            data_pagamento = str(df_tabelionato['DataExtracao'].dropna().iloc[0])[:10]

            # Base de custas
            df_custas = self._carregar_base_custas()

            # 2. Filtrar MAX por status aberto
            self.logger.info("2. Filtrando MAX por status aberto...")
            df_max_aberto = self._filtrar_max_status_aberto(df_max)

            # 3. Identificar diferença (MAX - Tabelionato)
            self.logger.info("3. Identificando protocolos apenas na MAX...")
            df_diferenca = self._identificar_diferenca_max_tabelionato(df_max_aberto, df_tabelionato)

            # Validações
            validacao_origem_max = localizar_chaves_ausentes(df_diferenca, df_max_aberto)
            if getattr(validacao_origem_max, "possui_inconsistencias", False):
                log_validation_presence(
                    "Baixa - presença na base MAX",
                    validacao_origem_max.total_verificado,
                    validacao_origem_max.amostras_inconsistentes,
                )
                resumo = resumir_amostras(validacao_origem_max.amostras_inconsistentes)
                raise ValueError(
                    f"Alguns protocolos identificados para baixa não estão mais presentes na base MAX: {resumo}"
                )
            else:
                log_validation_presence("Baixa - presença na base MAX", validacao_origem_max.total_verificado, [])

            validacao_chaves = localizar_chaves_presentes(df_diferenca, df_tabelionato)
            if getattr(validacao_chaves, "possui_inconsistencias", False):
                log_validation_result(
                    "Baixa - protocolos MAX vs Tabelionato",
                    validacao_chaves.total_verificado,
                    validacao_chaves.amostras_inconsistentes,
                )
                resumo = resumir_amostras(validacao_chaves.amostras_inconsistentes)
                raise ValueError(f"Foi identificada sobreposição entre protocolos: {resumo}")
            else:
                log_validation_result("Baixa - protocolos MAX vs Tabelionato", validacao_chaves.total_verificado, [])

            if len(df_diferenca) == 0:
                mensagem = "Nenhum protocolo em aberto da MAX ficou sem retorno do Tabelionato."
                self.logger.warning(mensagem)
                sucesso = True
                return ResultadoBaixa(
                    status="sem_registros",
                    mensagem=mensagem,
                    duracao=(datetime.now() - inicio).total_seconds(),
                )

            # 4. Enriquecer com custas
            self.logger.info("4. Enriquecendo com dados de custas...")
            df_enriquecido, df_checagem = self._enriquecer_com_custas(df_diferenca, df_custas)

            total_exportados = len(df_enriquecido)
            total_nao_exportados = len(df_checagem)

            if total_exportados == 0:
                mensagem = (
                    f"Após o enriquecimento, nenhum protocolo permaneceu elegível para baixa. "
                    f"Todos os {total_nao_exportados:,} registros estão pendentes para análise."
                )
                self.logger.warning(mensagem)
                self._salvar_checagem(df_checagem)
                log_metrics(
                    "Baixa Tabelionato",
                    {
                        "Registros MAX carregados": f"{len(df_max):,}",
                        "Registros Tabelionato carregados": f"{len(df_tabelionato):,}",
                        "Diferença MAX - Tabelionato": f"{len(df_diferenca):,}",
                        "Exportados para baixa": f"{total_exportados:,}",
                        "Pendentes por custas": f"{total_nao_exportados:,}",
                    },
                )
                sucesso = True
                return ResultadoBaixa(
                    status="sem_registros",
                    mensagem=mensagem,
                    total_nao_exportados=total_nao_exportados,
                    duracao=(datetime.now() - inicio).total_seconds(),
                )

            # 5. Gerar layout final
            self.logger.info("5. Gerando layout final...")
            df_final = self._gerar_layout_final(df_enriquecido, data_pagamento)

            # 6. Salvar resultado
            self.logger.info("6. Salvando resultado...")
            arquivo_final = self._salvar_resultado(df_final)

            self.logger.info("7. Registrando protocolos sem match...")
            self._salvar_checagem(df_checagem)

            resultado = ResultadoBaixa(
                status="sucesso",
                mensagem="Processo de baixa concluído com sucesso.",
                arquivo_final=arquivo_final,
                total_exportados=total_exportados,
                total_nao_exportados=total_nao_exportados,
                duracao=(datetime.now() - inicio).total_seconds(),
            )

            log_metrics(
                "Baixa Tabelionato",
                {
                    "Registros MAX carregados": f"{len(df_max):,}",
                    "Registros Tabelionato carregados": f"{len(df_tabelionato):,}",
                    "Diferença MAX - Tabelionato": f"{len(df_diferenca):,}",
                    "Exportados para baixa": f"{total_exportados:,}",
                    "Pendentes por custas": f"{total_nao_exportados:,}",
                },
            )

            self.logger.info("Arquivo final: %s", resultado.arquivo_final)
            self.logger.info("Total de registros exportados: %s", total_exportados)
            self.logger.info("Registros pendentes de custas: %s", total_nao_exportados)

            sucesso = True
            return resultado

        except Exception:
            self.logger.exception("Erro no processo de baixa")
            raise
        finally:
            log_session_end("Baixa Tabelionato", success=sucesso)


def executar_processo_baixa() -> ResultadoBaixa:
    """Função wrapper para compatibilidade com main.py."""
    processor = BaixaProcessor()
    return processor._executar_processo_baixa()


def main() -> int:
    """Função principal para execução standalone."""
    try:
        resultado = executar_processo_baixa()
    except Exception as exc:
        linhas = [
            "[ERRO] Processo de baixa não concluído.",
            "",
            f"Detalhes: {exc}",
        ]
        print_section("BAIXA - TABELIONATO", linhas, leading_break=False)
        return 1

    if resultado.status == "sucesso":
        linhas = [
            "[STEP] Baixa Tabelionato",
            "",
            f"Registros exportados: {format_int(resultado.total_exportados)}",
        ]
        if resultado.total_nao_exportados:
            linhas.append(f"Pendentes registrados em log: {format_int(resultado.total_nao_exportados)}")
        if resultado.arquivo_final:
            linhas.extend(["", f"Arquivo exportado: {resultado.arquivo_final}"])
        if resultado.duracao:
            linhas.append(f"Duração: {format_duration(resultado.duracao)}")

        print_section("BAIXA - TABELIONATO", linhas, leading_break=False)
        return 0

    if resultado.status == "sem_registros":
        mensagem = resultado.mensagem or "Nenhum protocolo elegível para baixa."
        linhas = [
            "[STEP] Baixa Tabelionato",
            "",
            mensagem,
        ]
        if resultado.total_nao_exportados:
            linhas.append(f"Pendentes registrados em log: {format_int(resultado.total_nao_exportados)}")
        if resultado.duracao:
            linhas.append(f"Duração: {format_duration(resultado.duracao)}")

        print_section("BAIXA - TABELIONATO", linhas, leading_break=False)
        return 0

    mensagem = resultado.mensagem or "Processo de baixa não concluído."
    linhas = [
        "[ERRO] Processo de baixa não concluído.",
        "",
        mensagem,
    ]
    if resultado.duracao:
        linhas.append(f"Duração: {format_duration(resultado.duracao)}")
    print_section("BAIXA - TABELIONATO", linhas, leading_break=False)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
