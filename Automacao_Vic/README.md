# Pipeline VIC/MAX

> Sistema de processamento de dados para cruzamento VIC × MAX

## Início Rápido

```cmd
# 1. Executar pipeline
run_pipeline.bat

# 2. Escolher opção:
#    1 = Pipeline completo (com extração)
#    2 = Pipeline completo (sem extração)
#    3 = Apenas extração
```

## Estrutura

```
VIC/
├── main.py                 # Orquestrador principal
├── config.yaml             # Configurações centralizadas
├── run_pipeline.bat        # Script de execução
├── src/
│   ├── core/               # Componentes base
│   │   ├── base_processor.py
│   │   ├── file_manager.py
│   │   └── packager.py
│   ├── processors/         # Processadores de dados
│   │   ├── vic.py          # Tratamento VIC
│   │   ├── max.py          # Tratamento MAX
│   │   ├── batimento.py    # VIC - MAX
│   │   ├── devolucao.py    # MAX - VIC
│   │   ├── baixa.py        # Processamento baixas
│   │   └── enriquecimento.py
│   ├── utils/              # Utilitários
│   └── config/             # Carregamento de config
├── scripts/                # Scripts de extração
│   ├── extrair_email.py    # Extrai VIC do email
│   ├── extrair_basemax.py  # Extrai MAX do SQL
│   └── extrair_judicial.py # Extrai Judicial do SQL
├── tests/                  # Testes
└── docs/                   # Documentação
```

## Fluxo de Processamento

```
Extração → Tratamento VIC → Tratamento MAX → Batimento → Enriquecimento → Baixa → Devolução
```

## Configuração

1. Copie `.env.example` para `.env` e configure credenciais
2. Ajuste `config.yaml` se necessário

## Comandos CLI

```bash
# Pipeline completo
python main.py --pipeline-completo

# Pipeline sem extração (usa arquivos existentes)
python main.py --pipeline-completo --skip-extraction

# Apenas extração
python main.py --extrair-bases
```

## Resultados

Os arquivos gerados ficam em `data/output/`:
- `vic_tratada/` - Base VIC tratada
- `max_tratada/` - Base MAX tratada
- `batimento/` - Registros VIC não encontrados em MAX
- `devolucao/` - Registros MAX não encontrados em VIC
- `baixa/` - Registros para baixa
- `enriquecimento/` - Contatos enriquecidos

## Documentação

- [FLUXO.md](docs/FLUXO.md) - Diagrama do fluxo
- [INSTALACAO.md](docs/INSTALACAO.md) - Guia de instalação
