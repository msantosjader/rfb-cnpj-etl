# db/sqlite_loader.py

"""
Módulo para carregamento de dados no banco de dados SQLite.
"""

import gc
import sqlite3
from queue import Queue
from ..config import QUEUE_SIZE, DEBUG_LOG
from threading import Thread
from typing import Optional
from ..utils.logger import print_log
from ..utils.progress import pbar, update_progress
from ..utils.db_batch_producer import produce_batches
from ..utils.db_transformers import transform_batch


def consume_batches(insertion_queue, db_path: str, total_records: int, low_memory: bool):
    """
    Consome lotes da fila e os insere no banco de dados SQLite dentro de uma única transação.
    """
    progress = None
    if not DEBUG_LOG:
        progress = pbar(total=total_records)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("PRAGMA journal_mode=MEMORY;")
    cursor.execute("PRAGMA synchronous=OFF;")
    cursor.execute("PRAGMA foreign_keys=OFF;")

    inserted_total = 0

    try:
        cursor.execute("BEGIN TRANSACTION")

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

            verb = "INSERT OR IGNORE" if table == "empresa" else "INSERT"
            placeholders = ",".join(["?"] * len(columns))
            col_names = ",".join(columns)
            sql = f"{verb} INTO {table} ({col_names}) VALUES ({placeholders})"

            try:
                cursor.executemany(sql, rows)
            except Exception as insert_err:
                print_log(f"ERRO AO INSERIR NO SQLITE (tabela {table}): {insert_err}", level="error")

            if table != 'estabelecimento_cnae_sec':
                inserted_total += len(rows)

                update_progress(
                    rows_inserted=len(rows),
                    accumulated_total=inserted_total,
                    filename=item["filename"],
                    insertion_queue=insertion_queue,
                    queue_size_max=QUEUE_SIZE,
                    total=total_records,
                    debug=DEBUG_LOG,
                    bar=progress
                )

            insertion_queue.task_done()
            if low_memory:
                gc.collect()

        conn.commit()

    except Exception as e:
        print_log(f"ERRO FATAL NA CARGA SQLITE: {e}", level="error")

    finally:
        if progress:
            progress.close()
        conn.close()


def run_sqlite_loader(files_dir: str, db_path: str, total_records: int, low_memory: Optional[bool] = False):
    """
    Inicia o processo de carga de dados para o SQLite.
    """
    print_log(f"REALIZANDO CARGA NO BANCO DE DADOS SQLITE...", level="task")
    insertion_queue = Queue(maxsize=QUEUE_SIZE)

    writer = Thread(target=consume_batches, args=(insertion_queue, db_path, total_records, low_memory))
    writer.start()

    try:
        produce_batches(files_dir, insertion_queue, engine="sqlite", low_memory=low_memory)
    finally:
        insertion_queue.put(None)
        writer.join()
        print_log(f"CARGA DE DADOS CONCLUÍDA", level="success")
