# Pipeline Tabelionato

> Sistema de processamento de dados para cruzamento Tabelionato × MAX

## Início Rápido

```cmd
# 1. Executar pipeline
run_pipeline.bat

# 2. Escolher opção:
#    2 = Fluxo completo (extração > tratamento > batimento > baixa)
#    5 = Processar apenas MAX
#    6 = Processar apenas Tabelionato
#    7 = Processar apenas Batimento
#    8 = Processar apenas Baixa
```

## Estrutura

```
Tabelionato/
├── main.py                    # Em scripts/fluxo_completo.py
├── run_pipeline.bat           # Script de execução
├── requirements.txt
├── .env
├── src/
│   ├── core/                  # Componentes base
│   │   └── base_processor.py
│   ├── processors/            # Processadores de dados
│   │   ├── tabelionato.py     # Tratamento Tabelionato
│   │   ├── max.py             # Tratamento MAX
│   │   ├── batimento.py       # Tabelionato - MAX
│   │   └── baixa.py           # Baixas
│   └── utils/                 # Utilitários
├── scripts/
│   ├── fluxo_completo.py      # Orquestrador
│   ├── extrair_email.py       # Extrai Tabelionato do email
│   └── extrair_basemax.py     # Extrai MAX do SQL
├── tests/
└── docs/
```

## Fluxo de Processamento

```
Extração → Tratamento Tabelionato → Tratamento MAX → Batimento → Baixa
```

## Comandos CLI

```bash
# Definir PYTHONPATH
$env:PYTHONPATH = "."

# Tratamento
python src/processors/tabelionato.py
python src/processors/max.py

# Batimento
python src/processors/batimento.py

# Baixa
python src/processors/baixa.py
```

## Resultados

Os arquivos gerados ficam em `data/output/`:
- `tabelionato_tratada/` - Base Tabelionato tratada
- `max_tratada/` - Base MAX tratada
- `batimento/` - Pendências identificadas
- `baixa/` - Registros para baixa

## Documentação

- [docs/FLUXO.md](docs/FLUXO.md) - Diagrama do fluxo
