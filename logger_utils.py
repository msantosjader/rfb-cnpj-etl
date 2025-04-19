import logging
from datetime import datetime
import threading

# lock para evitar conflito de prints em multithread
print_lock = threading.Lock()

# momento em que a aplicação começou (para calcular tempo decorrido)
start_time = datetime.now()

def get_timestamp():
    now = datetime.now()
    elapsed = now - start_time
    formatted_elapsed = str(elapsed).split(".")[0]  # hh:mm:ss
    return now.strftime("%H:%M:%S"), formatted_elapsed


def print_log(msg: str, level: str = "info", time: bool = True) -> None:
    now, elapsed = get_timestamp()

    emojis = {
        "success": "✅ ",
        "info": "ℹ️",
        "warning": "⚠️",
        "error": "❌ ",
        "debug": "🐞",
        "start": "🚀",
        "done": "🏁",
        "search": "🔍",
        "web": "🌐",
        "folder": "📁",
        "task": "📋",
    }

    emoji = emojis.get(level, "")  # padrão se nível desconhecido

    if time:
        formatted_msg = f"|🕒 {now} |⏱️ {elapsed} |{emoji} {msg}"
    else:
        formatted_msg = f"{emoji} {msg}"

    with print_lock:
        print(formatted_msg)
