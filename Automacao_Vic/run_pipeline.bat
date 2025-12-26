@echo off
chcp 65001 >nul 2>&1
setlocal EnableDelayedExpansion

set "ROOT=%~dp0"
cd /d "%ROOT%"

:: ============================================
:: VERIFICAR E CRIAR AMBIENTE VIRTUAL SE NECESSÁRIO
:: ============================================
if not exist "venv\Scripts\python.exe" (
    echo ===============================================
    echo    CONFIGURACAO INICIAL DO PROJETO VIC/MAX
    echo ===============================================
    echo.
    
    python --version >nul 2>&1
    if errorlevel 1 (
        echo ERRO: Python nao encontrado no PATH.
        echo Por favor, instale Python 3.9+ e adicione ao PATH.
        pause
        exit /b 1
    )
    
    echo [1/3] Detectando Python...
    for /f "tokens=2 delims= " %%v in ('python --version 2^>^&1') do set PYVER=%%v
    echo       Python !PYVER! encontrado.
    echo.
    
    echo [2/3] Criando ambiente virtual...
    python -m venv venv
    if errorlevel 1 (
        echo ERRO: Falha ao criar ambiente virtual.
        pause
        exit /b 1
    )
    echo       Ambiente virtual criado.
    echo.
    
    echo [3/3] Instalando dependencias...
    call venv\Scripts\activate.bat
    pip install --upgrade pip >nul 2>&1
    pip install -r requirements.txt
    echo.
    echo ===============================================
    echo    CONFIGURACAO CONCLUIDA!
    echo ===============================================
    echo.
)

:: ============================================
:: ATIVAR AMBIENTE VIRTUAL
:: ============================================
call venv\Scripts\activate.bat
if errorlevel 1 (
    echo ERRO: Falha ao ativar o ambiente virtual.
    pause
    exit /b 1
)

:: ============================================
:: MENU PRINCIPAL
:: ============================================
:MENU
echo.
echo ===============================================
echo    PIPELINE VIC/MAX - PROCESSAMENTO DE DADOS
echo ===============================================
echo.
echo 1. Pipeline Completo (com extracao de bases)
echo 2. Pipeline Completo (sem extracao - usa arquivos existentes)
echo 3. Apenas Extracao das Bases
echo 4. Reinstalar Dependencias
echo 5. Sair
echo.
set /p opcao=Digite sua escolha (1-5): 

echo.
if "%opcao%"=="1" goto FULL_WITH_EXTRACT
if "%opcao%"=="2" goto PIPELINE_NO_EXTRACT
if "%opcao%"=="3" goto EXTRACT_ONLY
if "%opcao%"=="4" goto REINSTALL
if "%opcao%"=="5" goto EXIT

echo Opcao invalida.
goto MENU

:FULL_WITH_EXTRACT
echo Executando pipeline COMPLETO (com extracao)...
echo.
python main.py --pipeline-completo
if errorlevel 1 goto ERROR
goto END

:PIPELINE_NO_EXTRACT
echo Executando pipeline (sem extracao - usando arquivos existentes)...
echo.
python main.py --pipeline-completo --skip-extraction
if errorlevel 1 goto ERROR
goto END

:EXTRACT_ONLY
echo Executando apenas extracao das bases...
echo.
python main.py --extrair-bases
if errorlevel 1 goto ERROR
goto END

:REINSTALL
echo Reinstalando dependencias...
pip install --upgrade pip >nul 2>&1
pip install -r requirements.txt --force-reinstall
echo Dependencias reinstaladas.
goto END

:END
echo.
echo Operacao concluida com sucesso!
echo.
pause
goto MENU

:ERROR
echo.
echo ERRO: Falha na execucao! Verifique mensagens acima.
echo.
pause
exit /b 1

:EXIT
echo.
echo Obrigado por usar o Pipeline VIC/MAX!
echo.
exit /b 0
