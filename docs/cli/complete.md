# Download + Carga de Dados

## `complete`

Executa o pipeline completo para um único mês:

1. **Baixa** os arquivos `.zip` do site da Receita Federal.
2. **Carrega** os dados no banco (SQLite ou PostgreSQL).

### Comportamento Padrão

Se nenhuma flag for usada:

- **Mês**: último mês disponível (automaticamente detectado).
- **Banco**: SQLite, no caminho definido em `DATABASE_DEFAULT_PATH`.
- **Download**: pasta `DOWNLOAD_DEFAULT_PATH` com até 10 downloads simultâneos.
- **Limpeza**: arquivos `.zip` e `.part` existentes são mantidos.
- **Índices**: são criados após a carga.

### Flags (opcionais)

| Flag                | Tipo                  | Padrão                  | Descrição                                                                 |
|---------------------|-----------------------|-------------------------|---------------------------------------------------------------------------|
| `--month`           | `<MM/AAAA>`           | _último mês disponível_ | Mês de referência dos dados.                                              |
| `--engine`          | `sqlite` / `postgres` | `sqlite`                | Tipo do SGBD utilizado.                                                   |
| `--download-dir`    | `<path>`              | `data/downloads`        | Diretório onde os arquivos `.zip` serão salvos.                           |
| `--workers`         | `<int>`               | `10`                    | Nº de downloads simultâneos.                                              |
| `--clean`           | _flag_                | _desativado_            | Limpa arquivos `.zip`/`.part` da pasta de download antes de baixar.       |
| `--db-path`         | `<path>`              | `data/db/dados_cnpj.db` | Caminho do arquivo do banco de dados (usado no SQLite).                   |
| `--db-name`         | `<string>`            | `dados_cnpj`            | Nome do banco de dados (usado no Postgres).                               |
| `--skip-index`      | _flag_                | _desativado_            | Se usado, não cria índices ao final da carga de dados.                    |
| `--skip-validation` | _flag_                | _desativado_            | Se usado, ignora a verificação dos arquivos (local x site da RFB)         |
| `--low-memory`      | _flag_                | _desativado_            | Se usado, realiza garbage collects no decorrer da execução                |
| `--parallel`        | _flag_                | _desativado_            | Se usado, utiliza multi thread para a carga de dados (usado no Postgres)  |

### Exemplo

```bash
python cnpj.py complete --month 03/2025 --engine sqlite --clean
```