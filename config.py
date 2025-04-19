import multiprocessing
from pathlib import Path

# ---------------------------------------------------------------------------
# LINKS
# ---------------------------------------------------------------------------
CNPJ_DATA_URL = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"

# ---------------------------------------------------------------------------
# DIRETÓRIOS
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent  # Diretório base do projeto
DATA_DIR = BASE_DIR / "data"                # Diretório para dados (downloads e banco de dados)
DOWNLOAD_DIR = DATA_DIR / "downloads"       # Diretório onde os arquivos ZIP baixados serão armazenados
DATABASE_PATH = DATA_DIR / "dados_cnpj.db"  # Local do banco de dados

# ---------------------------------------------------------------------------
# DOWNLOADS
# ---------------------------------------------------------------------------
DOWNLOAD_CHUNK_SIZE = 8_194     # Tamanho (em bytes) de cada chunk ao fazer download em streaming
DOWNLOAD_CHUNK_TIMEOUT = 60     # Timeout (em segundos) para cada requisição de chunk
DOWNLOAD_MAX_RETRIES = 100      # Número máximo de tentativas de download antes de falhar definitivamente
DOWNLOAD_MAX_CONCURRENTS = 10   # Número de downloads simultâneos padrão
BROWSER_AGENTS = [              # Lista de user‑agents rotativos para as requisições HTTP
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Version/15.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/103.0.0.0 Safari/537.36",
]

# ---------------------------------------------------------------------------
# BANCO DE DADOS E DATAFRAME
# ---------------------------------------------------------------------------
DEFAULT_ENGINE = "sqlite"       # Engine padrão de banco de dados (por enquanto apenas SQLite)
USE_MEMORY_BUFFER = True        # Carregar CSVs em memória antes de processar (mais rápido, mais uso de RAM)
DATAFRAME_CHUNK_SIZE = 250_000  # Número de registros por batch ao inserir no banco
WORKER_THREADS = (              # Quantidade de threads de worker para pipeline de inserção
    max(1, multiprocessing.cpu_count() - 1))
QUEUE_SIZE = (                  # Tamanho da fila (back‑pressure) no pipeline de bulk‑insert
    max(2, WORKER_THREADS * 2))