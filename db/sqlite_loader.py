# sqlite_loader.py

"""
Módulo para carregamento de dados no banco de dados SQLite.
"""

import gc
import os
import sqlite3
from queue import Queue
from config import QUEUE_SIZE, FILES_WITH_INVALID_BYTES, DEBUG_LOG
from threading import Thread
from typing import Optional
from collections import defaultdict
from utils.logger import print_log
from utils.progress import pbar, update_progress
from utils.db_batch_producer import produce_batches
from utils.db_transformers import (filter_empresa_exceptions,
                                   normalize_numeric_br, normalize_dates, clean_null_bytes)



def consume_batches(insertion_queue, db_path: str, total_records: int, low_memory: bool):
    """
    Função para consumir lotes de dados da fila de inserção e inserir no banco de dados PostgreSQL.

    :params:
        insertion_queue: fila de inserção.
        db_path: caminho para o banco de dados SQLite.
        total_records: número total de registros a serem inseridos.
        low_memory: Se True, limpa a memória após cada inserção.
    """

    progress = None
    if not DEBUG_LOG:
        progress = pbar(total=total_records)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # PRAGMAs para acelerar o trabalho
    cursor.execute("PRAGMA journal_mode=MEMORY;")
    cursor.execute("PRAGMA synchronous=OFF;")
    cursor.execute("PRAGMA temp_store=MEMORY;")
    cursor.execute("PRAGMA cache_size=-128000;")
    cursor.execute("PRAGMA foreign_keys = OFF;")
    cursor.execute("PRAGMA locking_mode = EXCLUSIVE;")
    cursor.execute("PRAGMA automatic_index = OFF;")

    inserted_total = 0
    cnpj_exceptions = defaultdict(int)
    zip_exceptions = {
        os.path.splitext(name)[0].lower()  # remove extensão e deixa lowercase
        for name in FILES_WITH_INVALID_BYTES
    }

    try:
        cursor.execute("BEGIN")

        while True:
            item = insertion_queue.get()
            if item is None:
                break

            table = item["table"]
            columns = item["columns"]
            rows = item["rows"]
            filename = item["filename"]
            zip_file = os.path.splitext(os.path.basename(item["filename"]))[0].lower()

            if table == "empresa":
                rows = filter_empresa_exceptions(rows, columns, cnpj_exceptions)
                rows = normalize_numeric_br(rows, columns, ["capital_social"])
            elif table == "estabelecimento":
                rows = normalize_dates(rows, columns, [
                    "data_situacao_cadastral", "data_inicio_atividade", "data_situacao_especial"
                ])
                if zip_file in zip_exceptions:
                    rows = clean_null_bytes(rows, columns)

            elif table == "simples":
                rows = normalize_dates(
                    rows, columns,
                    ["data_opcao_simples", "data_exclusao_simples", "data_opcao_mei", "data_exclusao_mei"]
                )
            elif table == "socio":
                rows = normalize_dates(rows, columns, ["data_entrada_sociedade"])
                if zip_file in zip_exceptions:
                    rows = clean_null_bytes(rows, columns)

            placeholders = ",".join(["?"] * len(columns))
            col_names = ",".join(columns)
            sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"

            try:
                cursor.executemany(sql, rows)
            except Exception as insert_err:
                print_log(f"ERRO AO INSERIR NO SQLITE (tabela {table}): {insert_err}", level="error")


            inserted_total += len(rows)
            update_progress(
                rows_inserted=len(rows),
                accumulated_total=inserted_total,
                filename=filename,
                insertion_queue=insertion_queue,
                queue_size_max=QUEUE_SIZE,
                total=total_records,
                debug=DEBUG_LOG,
                bar=progress
            )

            insertion_queue.task_done()

            if low_memory:
                gc.collect()

        cursor.execute("COMMIT")

    finally:
        if not DEBUG_LOG:
            progress.close()
        conn.close()

def run_sqlite_loader(files_dir: str, db_path: str, total_records: int, low_memory: Optional[bool] = False):
    """
    Função para realizar a carga de dados no banco de dados SQLite.

    :params:
        files_dir: caminho para o diretório com os arquivos de dados.
        postgres_config: configurações do banco de dados PostgreSQL.
        total_records: número total de registros a serem inseridos.
        low_memory: se True, limpa a memória após cada inserção.
    """
    print_log(f"REALIZANDO CARGA NO BANCO DE DADOS SQLITE...", level="task")
    insertion_queue = Queue(maxsize=QUEUE_SIZE)

    writer = Thread(target=consume_batches, args=(insertion_queue, db_path, total_records, low_memory))
    writer.start()

    try:
        produce_batches(files_dir, insertion_queue, engine="sqlite", low_memory=low_memory)
    finally:
        writer.join()
        print_log(f"CARGA DE DADOS CONCLUÍDA", level="success")
