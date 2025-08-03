@echo off
title Download RFB-CNPJ

REM Navega para a raiz do projeto, nao importa como o script foi executado.
pushd %~dp0..

call .venv\Scripts\activate.bat
if %errorlevel% neq 0 (
    echo ERRO: Falha ao ativar o ambiente virtual.
    pause
    goto :cleanup
)

echo.
py -m src.rfb_cnpj_etl.main complete
echo.

:cleanup
popd
pause