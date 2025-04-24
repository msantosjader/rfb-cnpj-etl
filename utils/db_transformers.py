# db_transformers.py

"""
Transformadores de dados.
"""

import csv
from io import BytesIO, StringIO
from datetime import datetime
from typing import Dict, List, Optional, Union

EMPRESA_CNPJ_DUPLICATED = {"10959550", "11559186"}
MAX_OCURRENCES = 3


def filter_empresa_exceptions(
    rows: List[List[Union[str, None]]],
    columns: List[str],
    cnpj_exceptions: Dict[str, int]
) -> List[List[Union[str, None]]]:
    """
    Filtra linhas de empresas com CNPJ duplicado.

    :params:
        rows: lista de linhas a serem filtradas.
        columns: lista de nomes das colunas.
        cnpj_exceptions: dicionário para contar ocorrências de CNPJ duplicado.
    :return: lista de linhas filtradas.
    """
    try:
        idx_cnpj = columns.index("cnpj_basico")
        idx_razao = columns.index("razao_social")
        idx_natureza = columns.index("cod_natureza_juridica")
    except ValueError:
        return rows  # Campos não encontrados, retorna tudo

    def is_valid(row):
        cnpj = row[idx_cnpj]
        if cnpj not in EMPRESA_CNPJ_DUPLICATED:
            return True  # Se não for CNPJ monitorado, insere normalmente

        if cnpj_exceptions[cnpj] >= MAX_OCURRENCES:
            return True

        razao = row[idx_razao].strip()
        natureza = row[idx_natureza].strip()

        if razao or natureza not in ("0", "0000", ""):
            cnpj_exceptions[cnpj] += 1
            return True
        return False

    return list(filter(is_valid, rows))


def normalize_numeric_br(
    rows: List[Union[list, tuple]],
    columns: List[str],
    target_columns: Optional[List[str]] = None
) -> List[List]:
    """
    Normaliza valores numéricos do formato brasileiro (1.234,56 → 1234.56)
    apenas nas colunas especificadas.

    :params:
        rows: lista de tuplas ou listas representando os dados.
        columns: lista com o nome das colunas.
        target_columns: lista com nomes das colunas a normalizar. Se None, aplica a todas.
    :return: Lista de linhas normalizadas.
    """
    new_rows = []

    # define os índices das colunas-alvo
    col_indexes = (
        list(range(len(columns))) if target_columns is None
        else [i for i, col in enumerate(columns) if col in target_columns]
    )

    for row in rows:
        row = list(row)
        for i in col_indexes:
            val = row[i]
            if isinstance(val, str) and "," in val and val.replace(",", "").replace(".", "").isdigit():
                row[i] = val.replace(".", "").replace(",", ".")
        new_rows.append(row)

    return new_rows


def normalize_dates(
    rows: List[Union[list, tuple]],
    columns: List[str],
    target_columns: Optional[List[str]] = None
) -> List[List]:
    """
    Converte datas do formato 'YYYYMMDD' para 'YYYY-MM-DD'.
    Substitui valores inválidos por None.

    :params:
        rows: linhas de dados.
        columns: lista com o nome das colunas.
        target_columns: lista com nomes das colunas a normalizar. Se None, aplica a todas.
    :return: novas linhas com datas normalizadas
    """
    if target_columns is None:
        date_columns = [i for i, col in enumerate(columns) if col.startswith("data_")]
    else:
        date_columns = [i for i, col in enumerate(columns) if col in target_columns]

    new_rows = []
    for row in rows:
        new_row = list(row)
        for i in date_columns:
            val = new_row[i]
            if isinstance(val, str):
                val = val.strip()
                if val in ("00000000", "", " ", "0"):
                    new_row[i] = None
                elif len(val) == 8 and val.isdigit():
                    try:
                        new_row[i] = datetime.strptime(val, "%Y%m%d").date()
                    except ValueError:
                        new_row[i] = None
        new_rows.append(new_row)
    return new_rows


def clean_null_bytes(
    rows: List[List[Union[str, None]]],
    columns: Optional[List[str]] = None,
    target_columns: Optional[List[str]] = None
) -> List[List[Union[str, None]]]:
    """
    Remove caracteres nulos (\x00) das colunas string especificadas.
    Se target_columns for None, limpa todos os campos string.

    :params:
        rows: lista de tuplas ou listas representando os dados.
        columns: lista com o nome das colunas.
        target_columns: lista com nomes das colunas a normalizar. Se None, aplica a todas.
    :return: lista de linhas normalizadas.
    """
    if target_columns is None:
        col_indexes = None
    else:
        col_indexes = [i for i, col in enumerate(columns) if col in target_columns]

    return [
        [   # converte valores nulos em string vazia
            val.replace("\x00", "") if isinstance(val, str) and (col_indexes is None or i in col_indexes) else val
            for i, val in enumerate(row)
        ]
        for row in rows
    ]


def convert_rows_to_csv_buffer(rows: List[List[Union[str, int, float, None]]]) -> BytesIO:
    """
    Converte uma lista de linhas em um buffer de bytes CSV.

    :param: linhas a serem convertidas.
    :return: buffer de bytes CSV.
    """
    text_buffer = StringIO()
    writer = csv.writer(text_buffer, delimiter=';', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')

    for row in rows:
        new_row = []
        for val in row:
            if val is None:
                new_row.append("")
            elif isinstance(val, str):
                # substitui caracteres inválidos no encoding alvo
                sanitized = val.encode("windows-1252", errors="ignore").decode("windows-1252")
                new_row.append(sanitized)
            else:
                new_row.append(str(val))
        writer.writerow(new_row)

    # transforma para BytesIO (como esperado pelo COPY)
    byte_buffer = BytesIO(text_buffer.getvalue().encode("windows-1252"))
    byte_buffer.seek(0)
    return byte_buffer
