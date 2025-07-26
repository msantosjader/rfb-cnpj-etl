# utils/db_batch_producer.py

import gc
from queue import Queue
from pathlib import Path
import zipfile
import csv
import time
from .logger import print_log
from db.schema import SCHEMA
from io import TextIOWrapper
from config import BATCH_SIZE, BATCH_RATIO
from typing import Optional, List, Dict
from threading import Thread


def get_targets_from_zip_name(zip_name: str) -> List[Dict]:
    zip_stem = Path(zip_name).stem.rstrip('0123456789')
    targets = []
    for table_name, definition in SCHEMA.items():
        if definition['source_file_stem'].lower() == zip_stem.lower():
            columns = [col[0] for col in definition['columns']]
            targets.append({'name': table_name, 'columns': columns})

    if not targets:
        raise ValueError(f"Arquivo ZIP '{zip_name}' não corresponde a nenhuma tabela no SCHEMA.")
    return targets


def _process_zip_file(zip_file: Path, insertion_queue: Queue, low_memory: bool = False):
    try:
        targets = get_targets_from_zip_name(zip_file.name)
        if not targets:
            return

        batches = {t['name']: [] for t in targets}
        columns_map = {t['name']: t['columns'] for t in targets}

        estab_cols_map = {}
        is_estab_file = any(t['name'] in ['estabelecimento', 'estabelecimento_cnae_sec'] for t in targets)
        if is_estab_file:
            estab_source_cols = [c[0] for c in SCHEMA['estabelecimento']['columns']]
            estab_cols_map = {
                'cnpj_basico': estab_source_cols.index('cnpj_basico'),
                'cnpj_ordem': estab_source_cols.index('cnpj_ordem'),
                'cnpj_dv': estab_source_cols.index('cnpj_dv'),
                'cnae_sec': estab_source_cols.index('cod_cnae_secundario'),
            }

        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            for file_info in zip_ref.infolist():
                try:
                    with zip_ref.open(file_info.filename) as raw_file:
                        reader = csv.reader(TextIOWrapper(raw_file, encoding="latin1"), delimiter=';')
                        for row in reader:
                            for target in targets:
                                table_name = target['name']

                                if table_name == 'estabelecimento_cnae_sec':
                                    cnaes_secundarios = row[estab_cols_map['cnae_sec']].split(',')
                                    for cnae in cnaes_secundarios:
                                        cnae_limpo = cnae.strip()
                                        if cnae_limpo:
                                            new_row = [
                                                row[estab_cols_map['cnpj_basico']],
                                                row[estab_cols_map['cnpj_ordem']],
                                                row[estab_cols_map['cnpj_dv']],
                                                cnae_limpo,
                                            ]
                                            batches[table_name].append(new_row)
                                else:
                                    batches[table_name].append(row)

                            for table_name, batch_list in batches.items():
                                ratio = BATCH_RATIO.get(table_name, 1.0)
                                batch_size = int(BATCH_SIZE * ratio)
                                if len(batch_list) >= batch_size:
                                    while insertion_queue.full(): time.sleep(0.05)
                                    insertion_queue.put({
                                        "table": table_name,
                                        "columns": columns_map[table_name],
                                        "rows": batch_list,
                                        "filename": str(zip_file)
                                    })
                                    batches[table_name] = []

                        for table_name, batch_list in batches.items():
                            if batch_list:
                                insertion_queue.put({
                                    "table": table_name,
                                    "columns": columns_map[table_name],
                                    "rows": batch_list,
                                    "filename": str(zip_file)
                                })

                except Exception as e:
                    print_log(f"Erro ao ler {file_info.filename} em {zip_file.name}: {e}", level="error")

    except Exception as e:
        print_log(f"Erro ao abrir {zip_file.name}: {e}", level="error")

    finally:
        if low_memory:
            gc.collect()


def produce_batches(files_dir: str, insertion_queue: Queue, engine: str, num_workers: Optional[int] = None,
                    parallel: bool = False, low_memory: bool = False):
    zip_files = sorted(Path(files_dir).glob("*.zip"))

    if parallel and engine == "postgres":
        threads = []
        for zip_file in zip_files:
            t = Thread(target=_process_zip_file, args=(zip_file, insertion_queue, low_memory))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()
    else:
        for zip_file in zip_files:
            _process_zip_file(zip_file, insertion_queue, low_memory)

    if engine == "sqlite":
        insertion_queue.put(None)
    elif engine == "postgres":
        for _ in range(num_workers or 1):
            insertion_queue.put(None)