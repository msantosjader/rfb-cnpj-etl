
# Comando `db load`

Realiza apenas a carga a partir de arquivos `.zip` já baixados.
---
### Comportamento Padrão

- **Mês**: último mês disponível
- **Banco**: SQLite
- **Índices**: criados ao final da carga

### Flags (opcionais)

| Flag              | Tipo                 | Padrão                     | Descrição                                                                |
|-------------------|----------------------|-----------------------------|--------------------------------------------------------------------------|
| `--engine`        | `sqlite` / `postgres`| `sqlite`                   | Tipo do SGBD.                                                            |
| `--month`         | `<MM/AAAA>`          | _último mês disponível_     | Mês a ser carregado.                                                     |
| `--download-dir`  | `<path>`             | `data/downloads/YYYY-MM`   | Pasta onde os arquivos `.zip` estão.                                     |
| `--db-path`         | `<path>`              | `data/db/dados_cnpj.db` | Caminho do arquivo do banco de dados (usado no SQLite).                   |
| `--db-name`         | `<string>`            | `dados_cnpj`            | Nome do banco de dados (usado no Postgres).                               |
| `--skip-index`      | _flag_                | _desativado_            | Se usado, não cria índices ao final da carga de dados.                   |
| `--skip-validation` | _flag_                | _desativado_            | Se usado, ignora a verificação dos arquivos (local x site da RFB)        |
| `--low-memory`      | _flag_                | _desativado_            | Se usado, realiza garbage collects no decorrer da execução               |
| `--parallel`        | _flag_                | _desativado_            | Se usado, utiliza multi thread para a carga de dados (usado no Postgres) |

## Exemplo

```bash
python cnpj.py db load --month 04/2025 --download-dir data/downloads --engine sqlite --skip-index
```

> É possível criar os índices depois com o comando `db index`.

---