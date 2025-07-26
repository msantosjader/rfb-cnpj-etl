# utils/logger.py

"""
Logger para o projeto.
"""

from datetime import datetime
import threading

# lock para evitar conflito de prints em multithread
print_lock = threading.Lock()

# momento em que a aplicação começou (para calcular tempo decorrido)
start_time = datetime.now()


def get_timestamp():
    """
    Retorna o timestamp atual e o tempo decorrido desde o início da aplicação.
    """
    now = datetime.now()
    elapsed = now - start_time
    hours, remainder = divmod(elapsed.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    formatted_elapsed = f"{hours:02}:{minutes:02}:{seconds:02}"
    return now.strftime("%H:%M:%S"), formatted_elapsed


def print_log(msg: str, level: str = None, time: bool = True) -> None:
    """
    Imprime uma mensagem no terminal.
    :params:
        msg: mensagem a ser impressa
        level: nível da mensagem (docs, warning, error, debug)
        time: se True, imprime o tempo decorrido desde o início da aplicação
    :return: None
    """
    now, elapsed = get_timestamp()

    emojis = {
        "success": "✅",
        "docs": "ℹ️",
        "warning": "⚠️",
        "error": "❌",
        "debug": "🐞",
        "start": "🚀",
        "done": "🏁",
        "search": "🔍",
        "web": "🌐",
        "folder": "📁",
        "task": "📋",
    }

    emoji = emojis.get(level, "")

    if time:
        formatted_msg = f"🕒 {now} |⏱️ {elapsed} |{emoji} {msg}"
    else:
        formatted_msg = f"{emoji} {msg}"

    with print_lock:
        print(formatted_msg)
