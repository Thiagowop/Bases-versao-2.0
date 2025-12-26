# Pipelines de Processamento de Dados

Repositório contendo os 3 pipelines de processamento de dados padronizados.

## Projetos

| Projeto | Descrição | Arquivos |
|---------|-----------|----------|
| [Automacao_Vic](./Automacao_Vic) | Pipeline VIC × MAX | 44 |
| [Emccamp](./Emccamp) | Pipeline EMCCAMP × MAX | 39 |
| [Automacao_Tabelionato](./Automacao_Tabelionato) | Pipeline Tabelionato × MAX | 25 |

## Estrutura Padrão

Todos os projetos seguem a mesma estrutura:

```
Projeto/
├── main.py / scripts/fluxo_completo.py
├── config.yaml
├── run_pipeline.bat
├── src/
│   ├── core/           # Componentes base
│   ├── processors/     # Processadores de dados
│   └── utils/          # Utilitários
├── scripts/            # Scripts de extração
├── tests/
└── docs/
```

## Como Executar

```cmd
cd Automacao_Vic   # ou Emccamp ou Automacao_Tabelionato
run_pipeline.bat
```

## Versão

- **v2.0** - Simplificação e padronização dos pipelines (Dez/2025)
