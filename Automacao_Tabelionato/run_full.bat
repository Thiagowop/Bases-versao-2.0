@echo off
:: ============================================
:: TABELIONATO - EXECUCAO COMPLETA (NAO-INTERATIVO)
:: ============================================
:: Este script executa o fluxo completo do pipeline:
:: 1. Configura ambiente virtual (se necessario)
:: 2. Extrai bases (Email + MAX via SQL)
:: 3. Trata dados Tabelionato e MAX
:: 4. Executa Batimento e Baixa
::
:: Uso: run_full.bat [--skip-extraction]
:: ============================================

chcp 65001 >nul 2>&1
setlocal EnableDelayedExpansion

set "ROOT=%~dp0"
cd /d "%ROOT%"

echo.
echo ===============================================
echo    TABELIONATO - PIPELINE COMPLETO
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
set "SKIP_FLAG="
if "%~1"=="--skip-extraction" set "SKIP_FLAG=--skip-extraction"

:: Executar pipeline
echo [EXEC] Iniciando pipeline completo...
echo.

if defined SKIP_FLAG (
    python main.py full --skip-extraction
) else (
    python main.py full
)

if errorlevel 1 (
    echo.
    echo [ERRO] Pipeline falhou! Verifique logs acima.
    exit /b 1
)

echo.
echo ===============================================
echo    PIPELINE COMPLETO - SUCESSO
echo    %date% %time%
echo ===============================================
exit /b 0
