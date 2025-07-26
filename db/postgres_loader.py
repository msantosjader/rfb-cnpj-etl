import gc
import psycopg2
from queue import Queue
from threading import Thread, Lock
from typing import Optional, Any, Dict
from config import QUEUE_SIZE, WORKER_THREADS, DEBUG_LOG
from utils.logger import print_log
from utils.progress import pbar, update_progress
from utils.db_batch_producer import produce_batches
from utils.db_transformers import transform_batch, convert_rows_to_csv_buffer


def consume_batches(insertion_queue, postgres_config: dict, thread_id: int,
                    progress_lock, shared_progress, low_memory: bool, total_records: int):
    """
    Função para consumir lotes de dados da fila de inserção e inserir no banco de dados PostgreSQL.
    """
    try:
        conn = psycopg2.connect(**postgres_config)
        conn.set_client_encoding("WIN1252")
        conn.autocommit = False
        cur = conn.cursor()

        while True:
            item = insertion_queue.get()
            if item is None:
                insertion_queue.task_done()
                break

            rows = transform_batch(item)
            if not rows:
                insertion_queue.task_done()
                continue

            table = item["table"]
            columns = item["columns"]

            buffer = None
            try:
                buffer = convert_rows_to_csv_buffer(rows)
                copy_sql = f'COPY "{table}" ({",".join(columns)}) FROM STDIN WITH (FORMAT csv, DELIMITER \';\', NULL \'\')'
                cur.copy_expert(copy_sql, buffer)
                conn.commit()

            except psycopg2.Error as db_error:
                conn.rollback()
                pg_error_message = db_error.pgerror
                pg_code = db_error.pgcode
                print_log(f"ERRO DE DB INSERINDO EM '{table}': {pg_error_message} (Código: {pg_code})"
                          f" ARQUIVO: {item['filename']}",
                          level="error")

            except Exception as e:
                print_log(f"ERRO INSERINDO EM '{table}': {e}"
                          f"ARQUIVO: {item['filename']}",
                          level="error")

            finally:
                if buffer:
                    buffer.close()

            if table != 'estabelecimento_cnae_sec':
                update_progress(
                    rows_inserted=len(rows),
                    filename=item['filename'],
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


def run_postgres_loader(files_dir: str, postgres_config: dict, total_records: int, parallel: Optional[bool] = True,
                        low_memory: Optional[bool] = False):
    """
    Função para realizar a carga de dados no banco de dados PostgreSQL.
    """
    print_log("REALIZANDO CARGA NO BANCO DE DADOS POSTGRES...", level="task")
    insertion_queue = Queue(maxsize=QUEUE_SIZE)

    progress_lock = Lock()
    shared_progress: Dict[str, Any] = {
        "inserted_total": 0,
        "queue_size": 0
    }

    if not DEBUG_LOG:
        progress = pbar(total=total_records)
        shared_progress["bar"] = progress

    workers = []
    num_threads = WORKER_THREADS if parallel else 1

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
