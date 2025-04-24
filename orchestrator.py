# orchestrator.py

"""
Orquestração do projeto.
"""

import os
from typing import Optional
from cnpj_data import CNPJDataScraper
from db import SQLiteBuilder, run_sqlite_loader, PostgresBuilder, run_postgres_loader
from utils.logger import print_log
from utils.zip_metadata import validate_zip_files, estimate_total_lines
from config import (
    DEFAULT_ENGINE,
    DEFAULT_PARALLEL,
    DEFAULT_LOW_MEMORY,
    DOWNLOAD_DIR,
    SQLITE_DB_PATH,
    POSTGRES
)


def run_orchestrator(
        command: Optional[str] = "load",
        engine: Optional[str] = DEFAULT_ENGINE,
        db_path: Optional[str] = SQLITE_DB_PATH,
        db_name: Optional[str] = POSTGRES["database"],
        month_year: Optional[str] = None,
        files_dir: Optional[str] = None,
        skip_indexes: bool = False,
        skip_validation: bool = False,
        parallel: bool = DEFAULT_PARALLEL,
        low_memory: bool = DEFAULT_LOW_MEMORY
):
    """
    Orquestração da carga no banco de dados.

    :params:
        command: comando a ser executado ("load" ou "indexes").
        engine: engine do banco de dados ("sqlite" ou "postgres").
        db_path: caminho para o banco de dados SQLite.
        db_name: nome do banco de dados Postgres.
        month_year: mês e ano a ser carregado ("MM/AAAA").
        files_dir: diretório com os arquivos CSV.
        skip_indexes: se deve pular a criação de índices.
        skip_validation: se deve pular a validação dos arquivos.
        parallel: se deve usar threads para processamento.
        low_memory: se deve usar baixa memória para processamento.
    """
    print_log("INICIANDO TAREFAS DO BANCO DE DADOS...", level="start")

    estimated_lines = None
    postgres_config = None

    # se for comando de carga, preparar diretórios e arquivos
    if command == "load":
        data = CNPJDataScraper()

        if month_year is None:
            month_year = data.get_latest()

        if files_dir is None:
            mm, aaaa = month_year.split('/')
            folder = f"{aaaa}-{mm}"
            files_dir = os.path.join(DOWNLOAD_DIR, folder)

        # validar dos arquivos na pasta
        if not skip_validation and not validate_zip_files(month_year, files_dir):
            print_log("EXECUÇÃO INTERROMPIDA. VERIFIQUE OS ARQUIVOS NO DIRETÓRIO LOCAL.", level="error")
            raise

        # estimar linhas totais para controlar o progresso
        estimated_lines = estimate_total_lines(files_dir) # 196_894_499

    # instanciar o builder adequado
    if engine == "sqlite":
        builder = SQLiteBuilder(db_path=db_path)

    elif engine == "postgres":
        postgres_config = POSTGRES.copy()
        if db_name and db_name != POSTGRES["database"]:
            postgres_config["database"] = db_name
        builder = PostgresBuilder(config=postgres_config)

    else:
        raise ValueError(f"ENGINE NÃO SUPORTADA: {engine}")

    # inicializa o script_sql se for init ou load
    if command in ("init", "load"):
        builder.initialize_schema()

    # carrega os dados (somente no comando load)
    if command == "load":
        if engine == "sqlite":
            run_sqlite_loader(
                files_dir=files_dir,
                db_path=db_path,
                total_records=estimated_lines,
                low_memory=low_memory
            )
        elif engine == "postgres":

            run_postgres_loader(
                files_dir=files_dir,
                postgres_config=postgres_config,
                total_records=estimated_lines,
                parallel=parallel,
                low_memory=low_memory
            )
        else:
            raise ValueError(f"ENGINE NÃO SUPORTADA: {engine}")

    if command == 'load':
        builder.patch_data()

    # cria os índices (em load ou index, exceto se skip=true)
    if command == "index" or (command == "load" and not skip_indexes):
        builder.create_indexes()

    # ativa FKs apenas se for carga
    if command == "load":
        builder.enable_foreign_keys()

    print_log(f"EXECUÇÃO FINALIZADA | {engine.upper()} | {month_year}", level="done")



if __name__ == "__main__":
    run_orchestrator()

