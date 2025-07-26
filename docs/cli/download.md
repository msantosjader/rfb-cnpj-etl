# Comando `download`

O comando `download` permite baixar localmente os arquivos ZIP de dados abertos do CNPJ diretamente do site da Receita
Federal.

---

## Comportamento Padrão

Se nenhuma flag for informada:

- Baixa apenas o **mês mais recente** disponível.
- Salva os arquivos na pasta `data/downloads` (valor de `DOWNLOAD_DEFAULT_PATH`).
- Usa até **10 downloads simultâneos** (valor de `DOWNLOAD_MAX_CONCURRENTS`).
- **Não remove** arquivos existentes (`.zip`, `.part`).

---

## Flags Disponíveis

| Flag             | Tipo         | Padrão           | Descrição                                                      |
|------------------|--------------|------------------|----------------------------------------------------------------|
| `--month`        | `<MM/AAAA>…` | Último mês       | Lista de meses para baixar. Ex: `--month 03/2025 04/2025`.     |
| `--download-dir` | `<path>`     | `data/downloads` | Diretório onde os arquivos `.zip` serão salvos.                |
| `--workers`      | `<int>`      | `10`             | Número máximo de downloads concorrentes.                       |
| `--clean`        | _flag_       | Inativo          | Se presente, remove arquivos `.zip` e `.part` antes de baixar. |

---

## Uso Básico

```bash
python cnpj.py download
```

Equivalente a: baixar o mês mais recente, salvar na pasta padrão, sem remover arquivos existentes, com 10 workers.

---

## Exemplos

- Baixar apenas o mais recente (padrão):

  ```bash
  python cnpj.py download
  ```

- Baixar um mês específico:

  ```bash
  python cnpj.py download --month 03/2025
  ```

- Baixar vários meses e salvar em outra pasta:

  ```bash
  python cnpj.py download \
    --month 01/2025 02/2025 03/2025 \
    --download-dir data/meus_downloads
  ```

- Forçar limpeza antes de baixar:

  ```bash
  python cnpj.py download --month 03/2025 --clean
  ```

- Limitar a 4 downloads simultâneos:

  ```bash
  python cnpj.py download --month 03/2025 --workers 4
  ```

- Combinar todas as opções:

  ```bash
  python cnpj.py download \
    --month 01/2025 02/2025 03/2025 \
    --download-dir data/meus_downloads \
    --clean \
    --workers 5
  ```

---

## Observações

- O mês deve ser informado no formato `MM/AAAA`.
- Caso omita `--month`, o script irá baixar sempre o mês mais recente disponível.
- Os valores padrão (`DOWNLOAD_DEFAULT_PATH`, `DOWNLOAD_MAX_CONCURRENTS`, etc.) podem ser alterados no arquivo
  `config.py`.
