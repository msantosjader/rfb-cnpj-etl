# config.py

"""
Constantes e configurações do projeto.
"""

import multiprocessing
from pathlib import Path

# ---------------------------------------------------------------------------
# DIRETÓRIOS
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parents[2]  # diretório base do projeto
DATA_DIR = BASE_DIR / "data"  # diretório para dados (downloads e banco de dados)
DOWNLOAD_DIR = DATA_DIR / "downloads"  # diretório onde os arquivos ZIP baixados serão armazenados

# ---------------------------------------------------------------------------
# LINKS
# ---------------------------------------------------------------------------
CNPJ_DATA_URL = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"

# ---------------------------------------------------------------------------
# BANCO DE DADOS
# ---------------------------------------------------------------------------
AVG_COMPRESSED_LINE_SIZE_BYTES = 35
ENGINE_OPTIONS = ["sqlite", "postgres"]  # opções de engines de banco de dados (já implementadas)
DEFAULT_ENGINE = "sqlite"  # engine padrão de banco de dados (por enquanto apenas SQLite)
DEFAULT_PARALLEL = True  # paralelismo de inserção no banco de dados
DEFAULT_LOW_MEMORY = False  # habilita o uso de memória limitada para inserção no banco

BATCH_SIZE = 200_000  # 50_000  # número de registros por batch ao inserir no banco
BATCH_RATIO = {  # proporção para utilizar em tabelas específicas
    "estabelecimento": 0.4  # Ex.: 250_000 * 0.5 = 125_000 para a tabela estabelecimento
}
WORKER_THREADS = max(1, multiprocessing.cpu_count() - 1)  # quantidade de threads de worker para pipeline de inserção
QUEUE_SIZE = max(2, WORKER_THREADS * 2) - 5  # tamanho da fila (back‑pressure) no pipeline inserção

# ---------------------------------------------------------------------------
# POSTGRESQL
# ---------------------------------------------------------------------------
POSTGRES = {
    "host": "localhost",
    "port": 5432,
    "user": "postgres",
    "password": "147258",  # sua_senha_aqui
    "database": "dados_cnpj"
}

# ---------------------------------------------------------------------------
# SQLITE
# ---------------------------------------------------------------------------
SQLITE_DB_PATH = DATA_DIR / "dados_cnpj.db"  # local do banco de dados

# ---------------------------------------------------------------------------
# DOWNLOADS
# ---------------------------------------------------------------------------
DOWNLOAD_CHUNK_SIZE = 8_194  # tamanho (em bytes) de cada chunk ao fazer download em streaming
DOWNLOAD_CHUNK_TIMEOUT = 60  # timeout (em segundos) para cada requisição de chunk
DOWNLOAD_MAX_RETRIES = 100  # número máximo de tentativas de download antes de falhar definitivamente
DOWNLOAD_MAX_CONCURRENTS = 10  # número de downloads simultâneos padrão
BROWSER_AGENTS = [  # lista de user‑agents rotativos para as requisições HTTP
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Version/15.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/103.0.0.0 Safari/537.36",
]

# ---------------------------------------------------------------------------
# PRINT_LOG
# ---------------------------------------------------------------------------
# True: 🕒 23:18:32 |⏱️ 0:06:37 |🐞  2.507.405 (  1.27%) | ESTABELECIMENTOS8.ZIP   | FILA: 22 / 22
# False: utiliza uma barra de progresso com tqdm
DEBUG_LOG = True
