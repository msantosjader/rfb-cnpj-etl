# progress.py

"""
Barra de progresso.
"""

from os.path import basename
from tqdm import tqdm
from utils.logger import print_log


def pbar(total: int, desc: str = "INSERINDO DADOS..."):
    return tqdm(
        total=total,
        desc=desc,
        unit="registros",
        dynamic_ncols=False,
        leave=False,
        bar_format="💾 {desc} {percentage:6.2f}% {bar} [{elapsed}]"
    )


def update_progress(rows_inserted: int,
                    filename: str,
                    insertion_queue,
                    queue_size_max: int,
                    total: int,
                    debug: bool = False,
                    shared: dict = None,
                    lock=None,
                    bar=None,
                    accumulated_total: int = None):
    """
    Atualiza barra de progresso ou imprime log de debug, de forma thread-safe se necessário.

    :params:
        rows_inserted: Quantidade de linhas inseridas nesse passo
        filename: Nome do arquivo sendo processado
        insertion_queue: Fila de inserção
        queue_size_max: Tamanho máximo da fila
        total: Total de registros a serem processados
        debug: Se True, imprime log detalhado em vez de usar barra
        shared: Dicionário compartilhado entre threads (opcional)
        lock: Lock para sincronização (opcional)
        bar: Instância da barra de progresso (opcional)
    """
    if shared is not None and lock is not None:
        with lock:
            shared["inserted_total"] += rows_inserted
            shared["queue_size"] = insertion_queue.qsize()
            inserted = shared["inserted_total"]
    elif accumulated_total is not None:
        inserted = accumulated_total
    else:
        inserted = rows_inserted

    percent = min(100.0, (inserted / total) * 100) if total else 0
    queue_size = insertion_queue.qsize()
    fname = basename(filename).upper()

    if not debug and bar:
        bar.update(rows_inserted)
    else:
        print_log(
            f"REGISTROS: {inserted:>12,.0f}".replace(",", ".") +
            f" ({percent:6.2f}%)"
            f" | {fname:<23}" +
            f" | FILA: {queue_size:>2} / {queue_size_max:<2}",
            level="debug"
        )