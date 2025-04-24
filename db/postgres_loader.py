# postgres_loader.py

"""
Módulo para carregamento de dados no banco de dados PostgreSQL.
"""

import gc
import os
import psycopg2
from collections import defaultdict
from queue import Queue
from threading import Thread, Lock
from typing import Optional
from config import QUEUE_SIZE, WORKER_THREADS, FILES_WITH_INVALID_BYTES, DEBUG_LOG
from utils.logger import print_log
from utils.progress import pbar, update_progress
from utils.db_batch_producer import produce_batches
from utils.db_transformers import (filter_empresa_exceptions, convert_rows_to_csv_buffer,
                                   normalize_numeric_br, normalize_dates, clean_null_bytes)


def consume_batches(insertion_queue, postgres_config: dict, thread_id: int,
                    progress_lock, shared_progress, low_memory: bool, total_records: int):
    """
    Função para consumir lotes de dados da fila de inserção e inserir no banco de dados PostgreSQL.

    :params:
        insertion_queue: fila de inserção.
        postgres_config: configurações do banco de dados PostgreSQL.
        thread_id: ID da thread.
        progress_lock: Lock para sincronizar a barra de progresso.
        shared_progress: dicionário compartilhado com a barra de progresso.
        low_memory: Se True, limpa a memória após cada inserção.
    """
    try:
        conn = psycopg2.connect(**postgres_config)
        conn.set_client_encoding("WIN1252")
        conn.autocommit = False
        cur = conn.cursor()

        cnpj_exceptions = defaultdict(int)
        zip_exceptions = {
            os.path.splitext(name)[0].lower()  # remove extensão e deixa lowercase
            for name in FILES_WITH_INVALID_BYTES
        }

        while True:
            item = insertion_queue.get()
            if item is None:
                insertion_queue.task_done()
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

            buffer = None
            try:
                buffer = convert_rows_to_csv_buffer(rows)
                copy_sql = f'COPY "{table}" ({", ".join(columns)}) FROM STDIN WITH (FORMAT csv, DELIMITER \';\', NULL \'\')'
                cur.copy_expert(copy_sql, buffer)
                conn.commit()
            except Exception as e:
                conn.rollback()
                print_log(f"ERRO INSERINDO EM '{table}': {e}"
                          f"ARQUIVO: {filename}",
                          level="error")

            finally:
                if buffer:
                    buffer.close()

            update_progress(
                rows_inserted=len(rows),
                filename=filename,
                insertion_queue=insertion_queue,
                queue_size_max=QUEUE_SIZE,
                shared=shared_progress,
                lock=progress_lock,
                total=total_records,
                debug=DEBUG_LOG
            )

            insertion_queue.task_done()
            if low_memory:
                gc.collect()

        cur.close()
        conn.close()

    except Exception as fatal:
        print_log(f"[THREAD-{thread_id}] ERRO FATAL: {fatal}", level="error")


def run_postgres_loader(files_dir: str, postgres_config: dict, total_records: int, parallel: Optional[bool] = True, low_memory: Optional[bool] = False):
    """
    Função para realizar a carga de dados no banco de dados PostgreSQL.
    :params:
        files_dir: caminho para o diretório com os arquivos de dados.
        postgres_config: configurações do banco de dados PostgreSQL.
        total_records: número total de registros a serem inseridos.
        parallel: se True, realiza a carga em paralelo.
        low_memory: se True, limpa a memória após cada inserção.
    """
    print_log("REALIZANDO CARGA NO BANCO DE DADOS POSTGRES...", level="task")
    insertion_queue = Queue(maxsize=QUEUE_SIZE)

    progress_lock = Lock()
    shared_progress = {
        "inserted_total": 0,
        "queue_size": 0
    }

    if not DEBUG_LOG:
        progress = pbar(total=total_records)
        shared_progress["bar"] = progress


    workers = []

    num_threads = WORKER_THREADS if parallel else 1 # se parallel desativado utiliza apenas 1 worker

    for i in range(num_threads):
        t = Thread(
            target=consume_batches,
            args=(insertion_queue, postgres_config, i + 1, progress_lock, shared_progress, low_memory, total_records)
        )
        t.start()
        workers.append(t)

    try:
        produce_batches(
            files_dir=files_dir,
            insertion_queue=insertion_queue,
            engine="postgres",
            num_workers=num_threads,
            parallel=parallel,
            low_memory=low_memory
        )
    finally:
        for _ in workers:
            insertion_queue.put(None)

        for t in workers:
            t.join()

        print_log("CARGA DE DADOS CONCLUÍDA", level="success")