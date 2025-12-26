"""Classe base abstrata para todos os processadores do pipeline."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Optional, Union
import pandas as pd
import zipfile


class BaseProcessor(ABC):
    """Classe base que define a interface padrão para todos os processadores.
    
    Todos os processadores (VIC, MAX, Batimento, Devolução, Baixa, Enriquecimento)
    devem herdar desta classe e implementar o método processar().
    """
    
    def __init__(self, config: Dict[str, Any], logger: Any):
        """Inicializa o processador.
        
        Args:
            config: Dicionário de configurações do projeto
            logger: Logger para registro de eventos
        """
        self.config = config
        self.logger = logger
        self.paths_config = config.get('paths', {})
    
    @abstractmethod
    def processar(self, *args, **kwargs) -> Dict[str, Any]:
        """Método principal de processamento.
        
        Cada processador deve implementar sua própria lógica aqui.
        
        Returns:
            Dicionário com estatísticas e resultados do processamento
        """
        pass
    
    def carregar_arquivo(
        self,
        caminho: Union[str, Path],
        encoding: str = 'utf-8',
        separator: str = ';',
        dtype: Any = str
    ) -> pd.DataFrame:
        """Carrega um arquivo CSV ou ZIP contendo CSV.
        
        Args:
            caminho: Caminho para o arquivo
            encoding: Codificação do arquivo
            separator: Separador de colunas
            dtype: Tipo de dados para conversão
            
        Returns:
            DataFrame com os dados carregados
        """
        caminho = Path(caminho)
        
        if not caminho.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {caminho}")
        
        if caminho.suffix.lower() == '.zip':
            return self._carregar_zip(caminho, encoding, separator, dtype)
        else:
            return pd.read_csv(
                caminho,
                encoding=encoding,
                sep=separator,
                dtype=dtype
            )
    
    def _carregar_zip(
        self,
        caminho: Path,
        encoding: str = 'utf-8',
        separator: str = ';',
        dtype: Any = str
    ) -> pd.DataFrame:
        """Carrega CSV de dentro de um arquivo ZIP."""
        with zipfile.ZipFile(caminho, 'r') as zf:
            csv_files = [n for n in zf.namelist() if n.lower().endswith('.csv')]
            if not csv_files:
                raise ValueError(f"Nenhum CSV encontrado em {caminho}")
            
            with zf.open(csv_files[0]) as fh:
                return pd.read_csv(
                    fh,
                    encoding=encoding,
                    sep=separator,
                    dtype=dtype
                )
    
    def exportar_zip(
        self,
        df: pd.DataFrame,
        output_dir: Path,
        prefix: str,
        timestamp: str,
        encoding: str = 'utf-8',
        separator: str = ';'
    ) -> Path:
        """Exporta DataFrame para CSV dentro de um ZIP.
        
        Args:
            df: DataFrame a exportar
            output_dir: Diretório de saída
            prefix: Prefixo do nome do arquivo
            timestamp: Timestamp para o nome
            encoding: Codificação do CSV
            separator: Separador de colunas
            
        Returns:
            Caminho do arquivo ZIP gerado
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        csv_name = f"{prefix}_{timestamp}.csv"
        zip_name = f"{prefix}_{timestamp}.zip"
        zip_path = output_dir / zip_name
        
        # Criar CSV temporário no ZIP
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            csv_content = df.to_csv(
                index=False,
                encoding=encoding,
                sep=separator
            )
            zf.writestr(csv_name, csv_content)
        
        self.logger.info(f"Exportado: {zip_path}")
        return zip_path
    
    def get_output_path(self, subdir: str) -> Path:
        """Retorna o caminho do diretório de saída para uma subpasta.
        
        Args:
            subdir: Nome da subpasta (ex: 'vic_tratada', 'batimento')
            
        Returns:
            Path do diretório
        """
        base = self.paths_config.get('output', {}).get('base', 'data/output')
        return Path(base) / subdir
    
    def get_input_path(self, subdir: str) -> Path:
        """Retorna o caminho do diretório de entrada para uma subpasta.
        
        Args:
            subdir: Nome da subpasta (ex: 'vic', 'max')
            
        Returns:
            Path do diretório
        """
        return Path(self.paths_config.get('input', {}).get(subdir, f'data/input/{subdir}'))
