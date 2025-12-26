# 📊 RELATÓRIO DE EXECUÇÃO DOS PIPELINES

**Data:** 25/12/2025  
**Objetivo:** Validar funcionamento real dos 3 pipelines e documentar resultados

---

## ✅ EMCCAMP - EXECUTADO COM SUCESSO

**Comando:** `run_pipeline_emccamp.bat 3` (Pipeline sem extração)  
**Status:** Exit code 0 ✅

### Fluxo Executado
1. Tratamento EMCCAMP
2. Tratamento MAX
3. Batimento
4. Baixa
5. Devolução
6. Enriquecimento

### Outputs Gerados
```
data/output/
├── emccamp_tratada/           ✅
├── max_tratada/               ✅
├── batimento/                 ✅
├── baixa/                     ✅
├── devolucao/                 ✅
├── enriquecimento_contato_emccamp/ ✅
└── inconsistencias/           ✅
```

---

## ✅ TABELIONATO - EXECUTADO COM SUCESSO

**Comando:** `fluxo_completo.bat`  
**Status:** Exit code 0 ✅

### Fluxo Executado (6 passos)
| Passo | Etapa | Resultado |
|-------|-------|-----------|
| 1/6 | Extração MAX | 230,123 registros em 69.93s |
| 2/6 | Extração Tabelionato | Concluído |
| 3/6 | Tratamento MAX | Concluído |
| 4/6 | Tratamento Tabelionato | Concluído |
| 5/6 | Batimento | Concluído |
| 6/6 | Baixa | Concluído |

### Outputs Gerados
```
data/output/
├── tabelionato_tratada/       ✅
├── max_tratada/               ✅
├── batimento/                 ✅
├── baixa/                     ✅
├── enriquecimento/            ✅
└── inconsistencias/           ✅
```

---

## ✅ VIC - EXECUTADO COM SUCESSO

**Comando:** `.\venv\Scripts\python.exe main.py --pipeline-completo`  
**Status:** Exit code 0 ✅

### Fluxo Executado

#### Extração (136.01s total)
| Etapa | Tempo | Detalhes |
|-------|-------|----------|
| VIC (Email) | 38.01s | 1 anexo baixado, data: 25/12/2025 12:30 |
| MAX (DB) | 75.58s | SQL Server MaxSmart |
| Judicial (DB) | 17.09s | ClientesJudiciais.zip |

#### Tratamento VIC
| Métrica | Valor |
|---------|-------|
| Registros originais | 1,007,701 |
| Inconsistências | 78 |
| Duplicatas removidas | 92 |
| **Registros finais** | **1,007,531** |
| Taxa aproveitamento | **99.98%** |
| Duração | 33.8s |

#### Tratamento MAX
| Métrica | Valor |
|---------|-------|
| Registros originais | 210,640 |
| Inconsistências (PARCELA inválida) | 8,051 |
| **Registros finais** | **202,589** |
| Taxa aproveitamento | **96.2%** |
| Duração | 3.4s |

#### Batimento VIC−MAX
| Métrica | Valor |
|---------|-------|
| VIC após filtros | 174,679 |
| Parcelas VIC ausentes no MAX | **2,041** |
| Judicial | 197 |
| Extrajudicial | 1,844 |
| Taxa de batimento | 1.17% |
| Consistência | ✓ OK |

### Outputs Gerados
```
data/output/
├── vic_tratada/               ✅
├── max_tratada/               ✅
├── batimento/                 ✅
├── baixa/                     ✅
├── devolucao/                 ✅
├── enriquecimento/            ✅
└── inconsistencias/           ✅
```

---

## 📋 RESUMO COMPARATIVO

| Projeto | Etapas | Status | Observação |
|---------|--------|--------|------------|
| **EMCCAMP** | 6 etapas | ✅ Funcionando | Mais bem estruturado |
| **VIC** | 4-6 etapas | ⚠️ Ambiente | v1/v2 coexistem |
| **Tabelionato** | 6 etapas | ✅ Funcionando | Regras de campanha |

---

## 🎯 CONCLUSÃO

**Todos os 3 projetos estão funcionais** e geram os outputs esperados:
- Bases tratadas (.zip)
- Batimentos (judicial/extrajudicial ou por campanha)
- Baixas
- Devoluções
- Enriquecimentos
- Inconsistências

---

*Relatório gerado automaticamente em 25/12/2025*
