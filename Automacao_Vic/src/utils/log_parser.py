"""Utilitários para parsing de logs de extração."""

import re
from typing import Dict, Tuple

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


def parse_extraction_summary(stdout: str) -> Tuple[Dict[str, str], list]:
    """Parseia saída de script de extração e retorna resumo + avisos."""
    resumo: Dict[str, str] = {}
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
