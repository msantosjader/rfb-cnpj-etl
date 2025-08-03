@echo off
title Setup do Projeto RFB-CNPJ

echo --- Script de Configuracao de Ambiente para Windows ---
echo.

pushd %~dp0..

REM --- Passo 1: Criacao do Ambiente Virtual ---
if exist .venv (
    echo Ambiente virtual '.venv' ja existe. Pulando etapa de criacao.
) else (
    echo Criando ambiente virtual em '.venv'...
    py -m venv .venv
    if %errorlevel% neq 0 (
        echo ERRO: Falha ao criar o ambiente virtual.
        pause
        goto :cleanup
    )
)
echo.

REM --- Passo 2: Instalacao das Dependencias ---
echo Instalando dependencias a partir de requirements.txt...
echo Isso pode demorar alguns minutos...
echo.

.venv\Scripts\pip.exe install -r requirements.txt
if %errorlevel% neq 0 (
    echo ERRO: Falha ao instalar as dependencias.
    pause
    goto :cleanup
)
echo.

echo -----------------------------------------------------------------
echo SUCESSO! O ambiente foi configurado corretamente.
echo -----------------------------------------------------------------
echo.
echo Agora voce pode executar os scripts:
echo.
echo - 'run_complete.bat' para executar o download e em seguida a carga de dados
echo.
echo ou, separadamente:
echo.
echo - 'run_download.bat' para realizar apenas o download dos arquivos mais recentes
echo.
echo - 'run_load.bat' para apenas criar e carregar o banco de dados com os arquivos baixados
echo.

:cleanup
popd
pause