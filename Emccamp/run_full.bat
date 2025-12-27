@echo off
:: ============================================
:: EMCCAMP - EXECUCAO COMPLETA (NAO-INTERATIVO)
:: ============================================
:: Este script executa o fluxo completo do pipeline:
:: 1. Configura ambiente virtual (se necessario)
:: 2. Extrai bases (EMCCAMP, MAX, Judicial, Baixas, Acordos)
:: 3. Trata dados EMCCAMP e MAX
:: 4. Executa Batimento, Baixa, Devolucao e Enriquecimento
::
:: Uso: run_full.bat [--skip-extraction]
:: ============================================

chcp 65001 >nul 2>&1
setlocal EnableDelayedExpansion

set "ROOT=%~dp0"
cd /d "%ROOT%"

echo.
echo ===============================================
echo    EMCCAMP - PIPELINE COMPLETO
echo    %date% %time%
echo ===============================================
echo.

:: Verificar/Criar ambiente virtual
if not exist "venv\Scripts\python.exe" (
    echo [SETUP] Ambiente virtual nao encontrado. Criando...
    python -m venv venv
    if errorlevel 1 (
        echo [ERRO] Falha ao criar ambiente virtual.
        exit /b 1
    )
    call venv\Scripts\activate.bat
    pip install --upgrade pip >nul 2>&1
    pip install -r requirements.txt
    echo [SETUP] Ambiente configurado com sucesso.
    echo.
) else (
    call venv\Scripts\activate.bat
)

:: Verificar argumentos
set "SKIP_EXTRACT="
if "%~1"=="--skip-extraction" set "SKIP_EXTRACT=1"

echo [EXEC] Iniciando pipeline completo...
echo.

if defined SKIP_EXTRACT (
    echo [1/5] Pulando extracao - usando arquivos existentes...
) else (
    echo [1/7] Extraindo bases...
    python main.py extract all
    if errorlevel 1 goto ERROR
)

echo [2/7] Tratando EMCCAMP...
python main.py treat emccamp
if errorlevel 1 goto ERROR

echo [3/7] Tratando MAX...
python main.py treat max
if errorlevel 1 goto ERROR

echo [4/7] Executando Batimento...
python main.py batimento
if errorlevel 1 goto ERROR

echo [5/7] Executando Baixa...
python main.py baixa
if errorlevel 1 goto ERROR

echo [6/7] Executando Devolucao...
python main.py devolucao
if errorlevel 1 goto ERROR

echo [7/7] Executando Enriquecimento...
python main.py enriquecimento
if errorlevel 1 goto ERROR

echo.
echo ===============================================
echo    PIPELINE COMPLETO - SUCESSO
echo    %date% %time%
echo ===============================================
exit /b 0

:ERROR
echo.
echo [ERRO] Pipeline falhou! Verifique logs acima.
exit /b 1
