# Pipeline Tabelionato

> Sistema de processamento de dados para cruzamento Tabelionato × MAX

## Inicio Rapido

### Opcao 1: Execucao Completa (Recomendado)
```cmd
run_full.bat
```

### Opcao 2: Menu Interativo (Desenvolvimento)
```cmd
run_pipeline.bat
```

## Scripts Disponiveis

| Script | Descricao |
|--------|-----------|
| `run_full.bat` | Execucao completa nao-interativa (setup + extracao + processamento) |
| `run_full.bat --skip-extraction` | Execucao completa usando arquivos existentes |
| `run_pipeline.bat` | Menu interativo para desenvolvimento |

## Estrutura do Projeto

```
Automacao_Tabelionato/
├── main.py                    # Orquestrador principal (CLI)
├── config.yaml                # Configuracoes centralizadas
├── run_full.bat               # Execucao completa
├── run_pipeline.bat           # Menu interativo
├── requirements.txt           # Dependencias Python
├── .env                       # Credenciais (nao versionado)
│
├── src/
│   ├── core/                  # Componentes base
│   │   ├── base_processor.py      # Classe base para processadores
│   │   └── extractor.py           # Extratores consolidados:
│   │                              #   - EmailDownloader (IMAP)
│   │                              #   - TabelionatoFileProcessor
│   │                              #   - TabelionatoEmailExtractor
│   │                              #   - MaxDBExtractor
│   │
│   ├── processors/            # Processadores de dados
│   │   ├── tabelionato.py         # Tratamento Tabelionato
│   │   ├── max.py                 # Tratamento MAX
│   │   ├── batimento.py           # Tabelionato - MAX
│   │   └── baixa.py               # Processamento de baixas
│   │
│   ├── utils/                 # Utilitarios
│   │   ├── helpers.py             # Funcoes auxiliares + normalizacoes
│   │   ├── archives.py            # Extracao ZIP/RAR com 7-Zip
│   │   └── logger_config.py       # Configuracao de logging
│   │
│   └── config/                # Carregamento de configuracao
│
├── scripts/                   # Scripts de extracao (wrappers)
│   ├── extrair_email.py           # Wrapper -> TabelionatoEmailExtractor
│   └── extrair_basemax.py         # Wrapper -> MaxDBExtractor
│
├── data/
│   ├── input/                 # Arquivos de entrada
│   │   ├── tabelionato/           # Base Tabelionato (email)
│   │   ├── tabelionato custas/    # Custas (email)
│   │   └── max/                   # Base MAX (SQL)
│   └── output/                # Arquivos de saida
│       ├── tabelionato_tratada/
│       ├── max_tratada/
│       ├── batimento/
│       └── baixa/
│
└── tests/                     # Testes automatizados
```

## Fluxo de Processamento

```
┌─────────────────────────────────────┐
│            EXTRACAO                 │
│  Email (Cobranca + Custas) | MAX    │
└─────────────────────────────────────┘
                  │
      ┌───────────┴───────────┐
      ▼                       ▼
┌─────────────┐         ┌─────────────┐
│  Tratamento │         │  Tratamento │
│ Tabelionato │         │     MAX     │
└─────────────┘         └─────────────┘
      │                       │
      └───────────┬───────────┘
                  ▼
           ┌─────────────┐
           │  Batimento  │
           │ TAB - MAX   │
           └─────────────┘
                  │
                  ▼
           ┌─────────────┐
           │    Baixa    │
           └─────────────┘
```

## Comandos CLI (main.py)

```bash
# Fluxo completo
python main.py full                    # Com extracao
python main.py full --skip-extraction  # Sem extracao

# Extracao
python main.py extract-email           # Extrai via email
python main.py extract-max             # Extrai MAX do SQL
python main.py extract-all             # Extrai todos

# Tratamento
python main.py treat-tabelionato       # Trata Tabelionato
python main.py treat-max               # Trata MAX
python main.py treat-all               # Trata todos

# Processamentos
python main.py batimento               # Executa batimento
python main.py baixa                   # Executa baixa
```

## Configuracao

### 1. Arquivo .env
```ini
# Email (Gmail)
EMAIL_USER=seu_email@gmail.com
EMAIL_APP_PASSWORD=xxxx xxxx xxxx xxxx
IMAP_SERVER=imap.gmail.com

# SQL Server
DB_SERVER=servidor
DB_DATABASE=database
DB_USER=usuario
DB_PASSWORD=senha
```

### 2. Arquivo config.yaml
Configuracoes de paths, colunas, campanhas (58, 78, 94) e opcoes de processamento.

## Extracao de Email (Consolidada)

A extracao de email foi consolidada em `src/core/extractor.py`:

| Classe | Funcao |
|--------|--------|
| `EmailDownloader` | Download via IMAP + salvamento de anexos |
| `TabelionatoFileProcessor` | Processamento de arquivos TXT/CSV |
| `TabelionatoEmailExtractor` | Orquestra download + processamento |
| `MaxDBExtractor` | Extracao do SQL Server |

### Funcoes de Normalizacao (helpers.py)

| Funcao | Descricao |
|--------|-----------|
| `normalize_text()` | Remove espacos extras |
| `normalize_data_tabelionato()` | Formata datas DD/MM/YYYY HH:MM:SS |
| `normalize_cep()` | Mantem apenas 8 digitos |
| `normalize_currency()` | Remove R$ e espacos |
| `normalize_bool()` | Normaliza True/False |
| `normalize_ascii_lower()` | Remove acentos para comparacoes |

## Resultados

| Diretorio | Conteudo |
|-----------|----------|
| `data/output/tabelionato_tratada/` | Base Tabelionato tratada |
| `data/output/max_tratada/` | Base MAX tratada |
| `data/output/batimento/` | Pendencias identificadas |
| `data/output/baixa/` | Registros para baixa |

## Testes

```bash
# Executar todos os testes
python -m pytest tests/ -v
```
