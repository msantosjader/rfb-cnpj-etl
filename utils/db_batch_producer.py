# db_batch_producer.py

"""
Script para produzir batches de dados para inserção no banco de dados.
"""

import gc
from queue import Queue
from pathlib import Path
import zipfile
import csv
import time
from .logger import print_log
from db.schema import tables
from io import TextIOWrapper
from config import BATCH_SIZE, BATCH_RATIO
from typing import Optional
from threading import Thread


def get_schema_from_zip_name(zip_name: str) -> tuple[str, list[str]]:
    """
    Obtém o nome da tabela e as colunas de um arquivo ZIP.

    :param: zip_name: nome do arquivo ZIP.
    :return: nome da tabela e lista de colunas.
    """
    zip_name_lower = zip_name.lower()
    for key, table in tables.items():
        if key in zip_name_lower:
            return table["nome_tabela"], list(table["colunas"].keys())
    raise ValueError(f"Arquivo ZIP '{zip_name}' não corresponde a nenhuma tabela conhecida.")


def _process_zip_file(zip_file: Path, insertion_queue: Queue, low_memory: bool = False):
    """
    Processa um arquivo ZIP, extraindo os dados e colocando-os na fila de inserção.
    :params:
        zip_file: caminho para o arquivo ZIP.
        insertion_queue: fila de inserção.
        low_memory: se True, desativa a memória compartilhada.
    :return: None
    """
    try:
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            for file_info in zip_ref.infolist():
                try:
                    with zip_ref.open(file_info.filename) as raw_file:
                        table_name, columns = get_schema_from_zip_name(zip_file.name)
                        reader = csv.reader(TextIOWrapper(raw_file, encoding="latin1"), delimiter=';')

                        batch = []
                        for row in reader:
                            if len(row) != len(columns):
                                continue
                            batch.append(row)

                            # ajusta o tamanho do batch conforme a engine, se necessário
                            ratio = BATCH_RATIO.get(table_name, 1.0)
                            batch_size = int(BATCH_SIZE * ratio)
                            if len(batch) >= batch_size:
                                while insertion_queue.full():
                                    time.sleep(0.05)

                                insertion_queue.put({
                                    "table": table_name,
                                    "columns": columns,
                                    "rows": batch,
                                    "filename": zip_file
                                })
                                batch = []

                        if batch:
                            insertion_queue.put({
                                "table": table_name,
                                "columns": columns,
                                "rows": batch,
                                    "filename": zip_file
                            })

                except Exception as e:
                    print_log(f"Erro ao ler {file_info.filename} em {zip_file.name}: {e}", level="error")

    except Exception as e:
        print_log(f"Erro ao abrir {zip_file.name}: {e}", level="error")

    finally:
        if low_memory:
            gc.collect()


def produce_batches(files_dir: str, insertion_queue: Queue, engine: str, num_workers: Optional[int] = None, parallel: bool = False, low_memory: bool = False):
    """
    Produz batches de dados para inserção no banco de dados.

    :params:
        files_dir: diretório onde os arquivos ZIP estão localizados.
        insertion_queue: fila de inserção.
        engine: nome do motor de banco de dados.
        num_workers: número de threads a serem usadas.
        parallel: se True, usa threads para processar arquivos ZIP.
        low_memory: se True, desativa a memória compartilhada.
    :return: None
    """
    zip_files = sorted(Path(files_dir).glob("*.zip"))

    if parallel and engine == "postgres":
        # modo paralelo
        threads = []
        for zip_file in zip_files:
            t = Thread(target=_process_zip_file, args=(zip_file, insertion_queue, low_memory))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()
    else:
        # modo sequencial
        for zip_file in zip_files:
            _process_zip_file(zip_file, insertion_queue, low_memory)

    # sinaliza fim da fila
    if engine == "sqlite":
        insertion_queue.put(None)
    elif engine == "postgres":
        for _ in range(num_workers or 1):
            insertion_queue.put(None)