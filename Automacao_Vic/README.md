# Pipeline VIC/MAX

> Sistema de processamento de dados para cruzamento VIC × MAX

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
Automacao_Vic/
├── main.py                 # Orquestrador principal (CLI)
├── config.yaml             # Configuracoes centralizadas
├── run_full.bat            # Execucao completa
├── run_pipeline.bat        # Menu interativo
├── requirements.txt        # Dependencias Python
├── .env                    # Credenciais (nao versionado)
│
├── src/
│   ├── core/               # Componentes base
│   │   ├── base_processor.py   # Classe base para processadores
│   │   ├── extractor.py        # Extratores consolidados (Email, SQL)
│   │   ├── file_manager.py     # Gerenciamento de arquivos
│   │   └── packager.py         # Empacotamento ZIP
│   │
│   ├── processors/         # Processadores de dados
│   │   ├── vic.py              # Tratamento VIC
│   │   ├── max.py              # Tratamento MAX
│   │   ├── batimento.py        # VIC - MAX (inclusao)
│   │   ├── devolucao.py        # MAX - VIC (devolucao)
│   │   ├── baixa.py            # Processamento de baixas
│   │   └── enriquecimento.py   # Enriquecimento de contatos
│   │
│   ├── utils/              # Utilitarios
│   │   ├── helpers.py          # Funcoes auxiliares + JudicialHelper
│   │   ├── logger.py           # Configuracao de logging
│   │   └── validator.py        # Validacao de dados
│   │
│   └── config/             # Carregamento de configuracao
│       └── loader.py           # ConfigLoader
│
├── scripts/                # Scripts de extracao (delegam para extractor.py)
│   ├── extrair_email.py        # Extrai VIC do email
│   ├── extrair_basemax.py      # Extrai MAX do SQL Server
│   └── extrair_judicial.py     # Extrai Judicial do SQL Server
│
├── data/
│   ├── input/              # Arquivos de entrada
│   │   ├── vic/                # Base VIC extraida
│   │   ├── max/                # Base MAX extraida
│   │   └── judicial/           # Clientes judiciais
│   └── output/             # Arquivos de saida
│       ├── vic_tratada/
│       ├── max_tratada/
│       ├── batimento/
│       ├── devolucao/
│       ├── baixa/
│       └── enriquecimento/
│
└── tests/                  # Testes automatizados
```

## Fluxo de Processamento

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Extracao   │ --> │ Tratamento  │ --> │ Batimento   │
│ VIC/MAX/JUD │     │  VIC + MAX  │     │  VIC - MAX  │
└─────────────┘     └─────────────┘     └─────────────┘
                                              │
                    ┌─────────────┐           │
                    │   Baixa     │ <---------┘
                    │ VIC baixado │
                    └─────────────┘
                          │
                    ┌─────────────┐
                    │  Devolucao  │
                    │  MAX - VIC  │
                    └─────────────┘
                          │
                    ┌─────────────┐
                    │Enriquecim.  │
                    │  Contatos   │
                    └─────────────┘
```

## Comandos CLI (main.py)

```bash
# Pipeline completo (com extracao)
python main.py --pipeline-completo

# Pipeline sem extracao (usa arquivos existentes)
python main.py --pipeline-completo --skip-extraction

# Apenas extracao
python main.py --extrair-bases

# Processos individuais
python main.py --vic                          # Tratar VIC
python main.py --max                          # Tratar MAX
python main.py --batimento vic.zip max.zip    # Batimento
python main.py --devolucao vic.zip max.zip    # Devolucao
```

## Configuracao

### 1. Arquivo .env
```ini
# Email (Gmail)
EMAIL_USER=seu_email@gmail.com
EMAIL_APP_PASSWORD=xxxx xxxx xxxx xxxx

# SQL Server
DB_SERVER=servidor
DB_DATABASE=database
DB_USER=usuario
DB_PASSWORD=senha
```

### 2. Arquivo config.yaml
Configuracoes de paths, colunas, filtros e opcoes de processamento.

## Resultados

| Diretorio | Conteudo |
|-----------|----------|
| `data/output/vic_tratada/` | Base VIC tratada |
| `data/output/max_tratada/` | Base MAX tratada |
| `data/output/batimento/` | Registros VIC nao encontrados em MAX (para inclusao) |
| `data/output/devolucao/` | Registros MAX nao encontrados em VIC (para devolucao) |
| `data/output/baixa/` | Registros para baixa |
| `data/output/enriquecimento/` | Contatos enriquecidos |

## Testes

```bash
# Executar todos os testes
python -m pytest tests/ -v

# Executar teste especifico
python -m pytest tests/test_batimento.py -v
```
