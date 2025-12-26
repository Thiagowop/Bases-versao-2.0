# Pipeline EMCCAMP

> Sistema de processamento de dados para cruzamento EMCCAMP × MAX

## Início Rápido

```cmd
# 1. Executar pipeline
run_pipeline.bat

# 2. Escolher opção:
#    1 = Pipeline completo (com extração)
#    2 = Extrair todas as bases
#    3 = Pipeline sem extração
```

## Estrutura

```
EMCCAMP/
├── main.py                    # Orquestrador principal
├── config.yaml                # Configurações centralizadas
├── run_pipeline.bat           # Script de execução
├── src/
│   ├── core/                  # Componentes base
│   │   ├── base_processor.py
│   │   └── file_manager.py
│   ├── processors/            # Processadores de dados
│   │   ├── emccamp.py         # Tratamento EMCCAMP
│   │   ├── max.py             # Tratamento MAX
│   │   ├── batimento.py       # EMCCAMP - MAX
│   │   ├── devolucao.py       # MAX - EMCCAMP
│   │   ├── baixa.py           # Baixas
│   │   └── contact_enrichment.py
│   ├── utils/                 # Utilitários
│   └── config/                # Carregamento de config
├── scripts/                   # Scripts de extração
│   ├── extrair_emccamp.py
│   ├── extrair_basemax.py
│   ├── extrair_judicial.py
│   ├── extrair_baixa_emccamp.py
│   └── extrair_doublecheck_acordo.py
├── tests/                     # Testes
└── docs/                      # Documentação
```

## Fluxo de Processamento

```
Extração → Tratamento EMCCAMP → Tratamento MAX → Batimento → Baixa → Devolução → Enriquecimento
```

## Comandos CLI

```bash
# Tratamento
python main.py treat all

# Batimento
python main.py batimento

# Baixa
python main.py baixa

# Devolução
python main.py devolucao

# Enriquecimento
python main.py enriquecimento

# Extração
python main.py extract all
```

## Resultados

Os arquivos gerados ficam em `data/output/`:
- `emccamp_tratado/` - Base EMCCAMP tratada
- `max_tratado/` - Base MAX tratada
- `batimento/` - Registros EMCCAMP não encontrados em MAX
- `devolucao/` - Registros MAX não encontrados em EMCCAMP
- `baixa/` - Registros para baixa
- `enriquecimento_contato_emccamp/` - Contatos enriquecidos

## Documentação

- [fluxo_completo.md](docs/fluxo_completo.md) - Diagrama do fluxo
