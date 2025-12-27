@echo off
chcp 65001 >nul 2>&1
setlocal EnableDelayedExpansion

set "ROOT=%~dp0"
cd /d "%ROOT%"

:: ============================================
:: VERIFICAR E CRIAR AMBIENTE VIRTUAL
:: ============================================
if not exist "venv\Scripts\python.exe" (
    echo ===============================================
    echo    CONFIGURACAO INICIAL - TABELIONATO
    echo ===============================================
    echo.

    python --version >nul 2>&1
    if errorlevel 1 (
        echo ERRO: Python nao encontrado no PATH.
        echo Instale Python 3.9+ e adicione ao PATH.
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
    echo ERRO: Falha ao ativar ambiente virtual.
    pause
    exit /b 1
)

:: ============================================
:: MODO NAO-INTERATIVO
:: ============================================
set "ONCE="
if not "%~1"=="" (
    set "OPT=%~1"
    set "ONCE=1"
    goto SELECT
)

:: ============================================
:: MENU PRINCIPAL
:: ============================================
:MENU
echo.
echo ===============================================
echo    PIPELINE TABELIONATO - MENU DE OPERACOES
echo ===============================================
echo.
echo  [FLUXOS COMPLETOS]
echo    1. Pipeline COMPLETO (extracao + tratamento + batimento + baixa)
echo    2. Pipeline SEM EXTRACAO (usa arquivos existentes)
echo.
echo  [EXTRACOES]
echo    3. Extrair Email (Tabelionato)
echo    4. Extrair MAX (SQL Server)
echo    5. Extrair TODOS
echo.
echo  [TRATAMENTOS]
echo    6. Tratar Tabelionato
echo    7. Tratar MAX
echo    8. Tratar TODOS
echo.
echo  [PROCESSAMENTOS]
echo    9. Executar Batimento
echo   10. Executar Baixa
echo.
echo  [MANUTENCAO]
echo   11. Reinstalar Dependencias
echo   12. Limpar Arquivos de Saida
echo    0. Sair
echo.
set /p OPT=Digite sua escolha (0-12):

:SELECT
if "%OPT%"=="1" goto FULL
if "%OPT%"=="2" goto FULL_NO_EXTRACT
if "%OPT%"=="3" goto EXTRACT_EMAIL
if "%OPT%"=="4" goto EXTRACT_MAX
if "%OPT%"=="5" goto EXTRACT_ALL
if "%OPT%"=="6" goto TREAT_TAB
if "%OPT%"=="7" goto TREAT_MAX
if "%OPT%"=="8" goto TREAT_ALL
if "%OPT%"=="9" goto BATIMENTO
if "%OPT%"=="10" goto BAIXA
if "%OPT%"=="11" goto REINSTALL
if "%OPT%"=="12" goto CLEAN
if "%OPT%"=="0" goto EXIT

echo Opcao invalida.
if defined ONCE goto EXIT
goto MENU

:: ============================================
:: FLUXOS COMPLETOS
:: ============================================
:FULL
echo.
echo ===============================================
echo    EXECUTANDO PIPELINE COMPLETO
echo ===============================================
python main.py full
if errorlevel 1 goto ERROR
goto END

:FULL_NO_EXTRACT
echo.
echo ===============================================
echo    EXECUTANDO PIPELINE (SEM EXTRACAO)
echo ===============================================
python main.py full --skip-extraction
if errorlevel 1 goto ERROR
goto END

:: ============================================
:: EXTRACOES
:: ============================================
:EXTRACT_EMAIL
echo.
echo Extraindo dados via Email...
python main.py extract-email
if errorlevel 1 goto ERROR
goto END

:EXTRACT_MAX
echo.
echo Extraindo dados do MAX (SQL Server)...
python main.py extract-max
if errorlevel 1 goto ERROR
goto END

:EXTRACT_ALL
echo.
echo Extraindo TODAS as bases...
python main.py extract-all
if errorlevel 1 goto ERROR
goto END

:: ============================================
:: TRATAMENTOS
:: ============================================
:TREAT_TAB
echo.
echo Tratando dados do Tabelionato...
python main.py treat-tabelionato
if errorlevel 1 goto ERROR
goto END

:TREAT_MAX
echo.
echo Tratando dados do MAX...
python main.py treat-max
if errorlevel 1 goto ERROR
goto END

:TREAT_ALL
echo.
echo Tratando TODAS as bases...
python main.py treat-all
if errorlevel 1 goto ERROR
goto END

:: ============================================
:: PROCESSAMENTOS
:: ============================================
:BATIMENTO
echo.
echo Executando Batimento Tabelionato x MAX...
python main.py batimento
if errorlevel 1 goto ERROR
goto END

:BAIXA
echo.
echo Executando Baixa...
python main.py baixa
if errorlevel 1 goto ERROR
goto END

:: ============================================
:: MANUTENCAO
:: ============================================
:REINSTALL
echo.
echo Reinstalando dependencias...
pip install --upgrade pip >nul 2>&1
pip install -r requirements.txt --force-reinstall
echo Dependencias reinstaladas.
goto END

:CLEAN
echo.
echo Limpando arquivos de saida...
if exist "data\output\max_tratada\*.zip" del /q "data\output\max_tratada\*.zip"
if exist "data\output\tabelionato_tratada\*.zip" del /q "data\output\tabelionato_tratada\*.zip"
if exist "data\output\batimento\*.zip" del /q "data\output\batimento\*.zip"
if exist "data\output\baixa\*.zip" del /q "data\output\baixa\*.zip"
if exist "data\output\inconsistencias\*.zip" del /q "data\output\inconsistencias\*.zip"
if exist "data\logs\*.log" del /q "data\logs\*.log"
echo Limpeza concluida!
goto END

:: ============================================
:: FINALIZACAO
:: ============================================
:END
echo.
echo Operacao concluida com sucesso!
echo.
if defined ONCE goto EXIT
pause
goto MENU

:ERROR
echo.
echo [ERRO] Falha na execucao! Verifique mensagens acima.
echo.
if defined ONCE exit /b 1
pause
goto MENU

:EXIT
echo.
echo Obrigado por usar o Pipeline Tabelionato!
echo.
exit /b 0
