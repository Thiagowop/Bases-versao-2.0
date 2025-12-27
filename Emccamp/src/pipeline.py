from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict

from src.config.loader import ConfigLoader, LoadedConfig
from src.processors import batimento as batimento_proc
from src.processors import baixa as baixa_proc
from src.processors import contact_enrichment as enrichment_proc
from src.processors import devolucao as devolucao_proc
from src.processors import emccamp as emccamp_proc
from src.processors import max as max_proc
from src.core.extractor import (
    EmccampExtractor,
    MaxDBExtractor,
    JudicialDBExtractor,
    BaixaExtractor,
    DoublecheckExtractor,
)


@dataclass(slots=True)
class Pipeline:
    """High-level access point for extraction and processing workflows."""

    loader: ConfigLoader = field(default_factory=ConfigLoader)
    _config: LoadedConfig | None = field(default=None, init=False, repr=False)

    def _get_config(self) -> LoadedConfig:
        if self._config is None:
            self._config = self.loader.load()
        return self._config

    # ---- Extraction layer -------------------------------------------------

    def extract_emccamp(self) -> None:
        config = self._get_config()
        extractor = EmccampExtractor(config, self.loader.base_path)
        extractor.extrair()

    def extract_max(self) -> None:
        config = self._get_config()
        extractor = MaxDBExtractor(config, self.loader.base_path)
        extractor.extrair()

    def extract_judicial(self) -> None:
        config = self._get_config()
        extractor = JudicialDBExtractor(config, self.loader.base_path)
        extractor.extrair()

    def extract_baixa(self) -> None:
        config = self._get_config()
        extractor = BaixaExtractor(config, self.loader.base_path)
        extractor.extrair()

    def extract_doublecheck(self) -> None:
        config = self._get_config()
        extractor = DoublecheckExtractor(config, self.loader.base_path)
        extractor.extrair()

    def extract_all(self) -> None:
        self.extract_emccamp()
        self.extract_baixa()
        self.extract_max()
        self.extract_judicial()
        self.extract_doublecheck()

    # ---- Treatment / processing -------------------------------------------

    def treat_emccamp(self) -> emccamp_proc.ProcessorStats:
        return emccamp_proc.run(self.loader)

    def treat_max(self) -> max_proc.MaxStats:
        return max_proc.run(self.loader)

    def treat_all(self) -> Dict[str, object]:
        return {
            "emccamp": self.treat_emccamp(),
            "max": self.treat_max(),
        }

    def batimento(self):
        return batimento_proc.run(self.loader)

    def baixa(self) -> None:
        baixa_proc.run(self.loader)

    def devolucao(self):
        """Executa processamento de devolução MAX - EMCCAMP."""
        return devolucao_proc.run(self.loader)

    def enriquecimento(self, dataset: str | None = None):
        return enrichment_proc.run(dataset, self.loader)
