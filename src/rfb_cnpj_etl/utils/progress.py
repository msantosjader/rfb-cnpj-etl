# utils/progress.py

"""
Barra de progresso.
"""

from os.path import basename
from tqdm import tqdm
from ..utils.logger import print_log


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
    # --- Se não estiver em modo debug, atualiza a barra e termina ---
    if not debug and bar:
        bar.update(rows_inserted)
        return

    # Calcula o total de registros inseridos até o momento
    if shared is not None and lock is not None:
        with lock:
            shared["inserted_total"] += rows_inserted
            current_inserted = shared["inserted_total"]
    elif accumulated_total is not None:
        current_inserted = accumulated_total
    else:
        return  # Não faz nada se não tiver como calcular o total

    # Calcula o percentual atual
    current_percent = min(100.0, (current_inserted / total) * 100) if total > 0 else 0

    # Determina o último percentual reportado para o LOG
    if shared is not None and lock is not None:
        # Modo multi-thread: pega do dicionário compartilhado
        last_log_percent = shared.get("last_log_percent", 0.0)
    else:
        # Modo single-thread: pega de um atributo da própria função
        if not hasattr(update_progress, 'last_log_percent'):
            update_progress.last_log_percent = 0.0
        last_log_percent = update_progress.last_log_percent

    # --- LÓGICA DE CONTROLE: SÓ IMPRIME O LOG SE AVANÇAR PELO MENOS 1% ---
    if (current_percent - last_log_percent) >= 0.5 or (current_percent == 100.0 and last_log_percent <= 100.0):
        # Atualiza o estado do último percentual reportado
        if shared is not None and lock is not None:
            with lock:
                shared["last_log_percent"] = current_percent
        else:
            update_progress.last_log_percent = current_percent

        # Imprime o log
        queue_size = insertion_queue.qsize()
        fname = basename(filename).upper()
        print_log(
            f"REGISTROS: {current_inserted:>12,.0f}".replace(",", ".") +
            f" ({current_percent:6.2f}%)"
            f" | {fname:<23}" +
            f" | FILA: {queue_size:>2} / {queue_size_max:<2}",
            level="debug"
        )