@echo off
chcp 65001 >nul 2>&1
setlocal
set "ROOT=%~dp0"
cd /d "%ROOT%"

if not exist "venv\Scripts\python.exe" (
    echo [AUTO] Ambiente virtual nao encontrado. Executando setup_project.bat...
    call setup_project.bat
)

if not exist "venv\Scripts\python.exe" (
    echo ERRO: Ambiente virtual nao encontrado.
    echo Execute primeiro o setup_project.bat.
    echo.
    pause
    exit /b 1
)

call venv\Scripts\activate.bat
if errorlevel 1 (
    echo ERRO: Falha ao ativar o ambiente virtual.
    pause
    exit /b 1
)

:MENU
echo ===============================================
echo    PIPELINE VIC/MAX - PROCESSAMENTO DE DADOS
echo ===============================================
echo.
echo 1. Pipeline Completo (com extracao)
echo 2. Pipeline Completo (sem extracao)
echo 3. Apenas Extracao das Bases
echo 4. Sair
echo.
set /p opcao=Digite sua escolha (1-4):

echo.
if "%opcao%"=="1" goto FULL_WITH_EXTRACT
if "%opcao%"=="2" goto PIPELINE
if "%opcao%"=="3" goto EXTRACT
if "%opcao%"=="4" goto EXIT

echo Opcao invalida.
echo.
goto MENU

:FULL_WITH_EXTRACT
echo [1/2] Extracao das bases...
python main.py --extrair-bases
if errorlevel 1 goto ERROR
echo.
echo [2/2] Executando pipeline completo...
python main.py --pipeline-completo --skip-extraction
if errorlevel 1 goto ERROR
goto END

:PIPELINE
echo Executando pipeline completo (sem extracao)...
python main.py --pipeline-completo --skip-extraction
if errorlevel 1 goto ERROR
goto END

:EXTRACT
echo Executando extracao das bases...
python main.py --extrair-bases
if errorlevel 1 goto ERROR
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
