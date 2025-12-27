# Pipeline EMCCAMP

> Sistema de processamento de dados para cruzamento EMCCAMP × MAX

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
Emccamp/
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
│   │   └── file_manager.py        # Gerenciamento de arquivos
│   │
│   ├── processors/            # Processadores de dados
│   │   ├── emccamp.py             # Tratamento EMCCAMP
│   │   ├── max.py                 # Tratamento MAX
│   │   ├── batimento.py           # EMCCAMP - MAX (inclusao)
│   │   ├── devolucao.py           # MAX - EMCCAMP (devolucao)
│   │   ├── baixa.py               # Processamento de baixas
│   │   └── contact_enrichment.py  # Enriquecimento de contatos
│   │
│   ├── utils/                 # Utilitarios
│   │   ├── helpers.py             # Funcoes auxiliares + JudicialHelper
│   │   ├── logger.py              # Configuracao de logging
│   │   └── io.py                  # Leitura/escrita de arquivos
│   │
│   ├── config/                # Carregamento de configuracao
│   │   └── loader.py              # ConfigLoader
│   │
│   └── pipeline.py            # Classe Pipeline (orquestra etapas)
│
├── scripts/                   # Scripts de extracao
│   ├── extrair_emccamp.py
│   ├── extrair_basemax.py
│   ├── extrair_judicial.py
│   ├── extrair_baixa_emccamp.py
│   └── extrair_doublecheck_acordo.py
│
├── data/
│   ├── input/                 # Arquivos de entrada
│   │   ├── emccamp/
│   │   ├── max/
│   │   ├── judicial/
│   │   ├── baixas/
│   │   └── doublecheck/
│   └── output/                # Arquivos de saida
│       ├── emccamp_tratado/
│       ├── max_tratado/
│       ├── batimento/
│       ├── devolucao/
│       ├── baixa/
│       └── enriquecimento_contato_emccamp/
│
└── tests/                     # Testes automatizados
```

## Fluxo de Processamento

```
┌─────────────────────────────────────────────────────────┐
│                      EXTRACAO                           │
│  EMCCAMP | MAX | Judicial | Baixas | DoubleCheck        │
└─────────────────────────────────────────────────────────┘
                          │
            ┌─────────────┴─────────────┐
            ▼                           ▼
     ┌─────────────┐             ┌─────────────┐
     │  Tratamento │             │  Tratamento │
     │   EMCCAMP   │             │     MAX     │
     └─────────────┘             └─────────────┘
            │                           │
            └─────────────┬─────────────┘
                          ▼
                   ┌─────────────┐
                   │  Batimento  │
                   │EMCCAMP - MAX│
                   └─────────────┘
                          │
            ┌─────────────┼─────────────┐
            ▼             ▼             ▼
     ┌─────────────┐┌─────────────┐┌─────────────┐
     │    Baixa    ││  Devolucao  ││Enriquecim.  │
     │             ││  MAX - EMP  ││  Contatos   │
     └─────────────┘└─────────────┘└─────────────┘
```

## Comandos CLI (main.py)

```bash
# Extracao
python main.py extract all          # Extrai todas as bases
python main.py extract emccamp      # Apenas EMCCAMP
python main.py extract max          # Apenas MAX
python main.py extract judicial     # Apenas Judicial

# Tratamento
python main.py treat all            # Trata EMCCAMP + MAX
python main.py treat emccamp        # Apenas EMCCAMP
python main.py treat max            # Apenas MAX

# Processamentos
python main.py batimento            # Executa batimento
python main.py baixa                # Executa baixa
python main.py devolucao            # Executa devolucao
python main.py enriquecimento       # Executa enriquecimento
```

## Configuracao

### 1. Arquivo .env
```ini
# Email
EMAIL_USER=seu_email@gmail.com
EMAIL_APP_PASSWORD=xxxx xxxx xxxx xxxx

# SQL Server
DB_SERVER=servidor
DB_DATABASE=database
DB_USER=usuario
DB_PASSWORD=senha
```

### 2. Arquivo config.yaml
Configuracoes de paths, colunas, filtros, empresa (CNPJ) e opcoes de processamento.

## Resultados

| Diretorio | Conteudo |
|-----------|----------|
| `data/output/emccamp_tratado/` | Base EMCCAMP tratada |
| `data/output/max_tratado/` | Base MAX tratada |
| `data/output/batimento/` | Registros EMCCAMP nao encontrados em MAX |
| `data/output/devolucao/` | Registros MAX nao encontrados em EMCCAMP |
| `data/output/baixa/` | Registros para baixa |
| `data/output/enriquecimento_contato_emccamp/` | Contatos enriquecidos |

## Testes

```bash
# Executar todos os testes
python -m pytest tests/ -v
```
