# utils/db_transformers.py

"""
Transformadores de dados.
"""

import csv
from io import BytesIO, StringIO
from datetime import datetime
from typing import List, Optional, Union, Callable, Any


def sanitize_for_sqlite(rows: List[List[Any]]) -> List[List[Any]]:
    """Remove o byte nulo e apara espaços em branco."""
    cleaned_rows = []
    for row in rows:
        new_row = [
            val.replace('\x00', '').strip() if isinstance(val, str) else val
            for val in row
        ]
        cleaned_rows.append(new_row)
    return cleaned_rows


def sanitize_for_postgres(rows: List[List[Any]]) -> List[List[Any]]:
    """Sanitiza para bancos de dados com encoding 'windows-1252'."""
    cleaned_rows = []
    for row in rows:
        new_row = []
        for val in row:
            if isinstance(val, str):
                s = val.replace('\x00', '').strip()
                # Remove caracteres incompatíveis com windows-1252
                val = s.encode("windows-1252", "ignore").decode("windows-1252")
            new_row.append(val)
        cleaned_rows.append(new_row)
    return cleaned_rows


def normalize_numeric_br(
        rows: List[Union[list, tuple]],
        columns: List[str],
        target_columns: Optional[List[str]] = None
) -> List[List]:
    """Normaliza valores numéricos do formato brasileiro (1.234,56 → 1234.56)."""
    new_rows = []
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
    """Converte datas do formato 'YYYYMMDD' para 'YYYY-MM-DD'."""
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


def convert_rows_to_csv_buffer(rows: List[List[Union[str, int, float, None]]]) -> BytesIO:
    """Converte uma lista de linhas em um buffer de bytes CSV para o COPY do Postgres."""
    text_buffer = StringIO()
    writer = csv.writer(text_buffer, delimiter=';', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
    writer.writerows(rows)
    byte_buffer = BytesIO(text_buffer.getvalue().encode("windows-1252"))
    byte_buffer.seek(0)
    return byte_buffer


def transform_batch(item: dict, sanitizer_func: Callable) -> List:
    """
    Aplica todas as transformações necessárias a um lote de dados.
    """
    table = item["table"]
    columns = item["columns"]
    rows = item["rows"]

    rows = sanitizer_func(rows)

    if table == "empresa":
        rows = normalize_numeric_br(rows, columns, ["capital_social"])

    elif table == "estabelecimento":
        rows = normalize_dates(rows, columns, [
            "data_situacao_cadastral", "data_inicio_atividade", "data_situacao_especial"
        ])

    elif table == "simples":
        rows = normalize_dates(
            rows, columns,
            ["data_opcao_simples", "data_exclusao_simples", "data_opcao_mei", "data_exclusao_mei"]
        )

    elif table == "socio":
        rows = normalize_dates(rows, columns, ["data_entrada_sociedade"])

    return rows
