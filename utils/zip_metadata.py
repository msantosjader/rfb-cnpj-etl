# utils/zip_metadata.py

"""
Módulo para lidar com metadados de arquivos ZIP.
"""

import io
import os
import zipfile
from collections import defaultdict
from cnpj_data.cnpj_public_data import CNPJDataScraper
from config import AVG_COMPRESSED_LINE_SIZE_BYTES
from .logger import print_log


def validate_zip_files(month_year: str, files_dir: str):
    """
    Valida os arquivos baixados.
    """
    print_log(f"VALIDAÇÃO DOS ARQUIVOS ZIP...", level="task")
    print_log(f"PERÍODO: {month_year}", level="info")
    print_log(f"LOCAL: {files_dir}", level="info")

    if not os.path.exists(files_dir):
        raise FileNotFoundError(f"DIRETÓRIO NÃO ENCONTRADO: {files_dir}")

    local_files = {
        fname: os.path.getsize(os.path.join(files_dir, fname))
        for fname in os.listdir(files_dir)
        if fname.lower().endswith('.zip')
    }
    print_log(f"{len(local_files)} ARQUIVOS NA PASTA LOCAL", level="folder")

    data = CNPJDataScraper()
    remote_metadata = data.get_metadata(month_year)
    remote_files = {
        info["filename"]: info["file_size"]
        for info in remote_metadata.values()
    }

    if local_files == remote_files:
        print_log(f"ARQUIVOS VALIDADOS", level="success")
        return True
    else:
        print_log("ERRO NA VALIDAÇÃO DOS ARQUIVOS", level="error")
        missing = set(remote_files) - set(local_files)
        exceding = set(local_files) - set(remote_files)
        mismatched = {
            fname for fname in local_files
            if fname in remote_files and local_files[fname] != remote_files[fname]
        }
        if missing:
            print(f"{len(missing)} ARQUIVO(S) FALTANDO:", *missing, sep="\n- ")
        if exceding:
            print(f"REMOVA DA PASTA:", *exceding, sep="\n- ")
        if mismatched:
            print(f"{len(mismatched)} ARQUIVO(S) COM TAMANHO(S) DIFERENTE(S):", *mismatched, sep="\n- ")
        return False


def arredondar_para(numero: float, fator: int = 10000) -> int:
    """
    Arredonda um número para o múltiplo mais próximo do fator especificado.
    """
    return int(round(numero / fator)) * fator


def estimate_total_lines_from_size(directory_path: str) -> int:
    """
    Estima o número total de linhas usando o tamanho dos arquivos.
    """
    print_log(f"Estimando número total de linhas para a pasta: {directory_path}", level="task")

    total_size_bytes = 0
    try:
        for filename in os.listdir(directory_path):
            if filename.lower().endswith('.zip'):
                total_size_bytes += os.path.getsize(os.path.join(directory_path, filename))
    except FileNotFoundError:
        print_log(f"Diretório não encontrado: {directory_path}", level="error")
        return 0

    if total_size_bytes == 0:
        print_log("Nenhum arquivo .zip encontrado ou o diretório está vazio.", level="info")
        return 0

    if AVG_COMPRESSED_LINE_SIZE_BYTES <= 0:
        print_log("A constante AVG_COMPRESSED_LINE_SIZE_BYTES deve ser maior que zero.", level="warning")
        return 0

    estimated_lines = total_size_bytes / AVG_COMPRESSED_LINE_SIZE_BYTES
    rounded_estimated_lines = arredondar_para(estimated_lines, fator=10000)

    print_log(
        f"Estimativa de linhas: {total_size_bytes / (1024 ** 3):.2f} GB de arquivos / {AVG_COMPRESSED_LINE_SIZE_BYTES} bytes/linha = ~{int(rounded_estimated_lines):,} de registros".replace(
            ",", "."), level="info")
    return int(rounded_estimated_lines)


def _count_lines(zip_path: str, encoding: str = 'latin1') -> int:
    """
    Conta as linhas do único arquivo dentro do ZIP.
    """
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            entry = z.infolist()[0]
            with z.open(entry, 'r') as bin_f, io.TextIOWrapper(bin_f, encoding=encoding, errors='ignore') as txt_f:
                return sum(1 for _ in txt_f)
    except Exception as e:
        print_log(f"ERRO AO CONTAR LINHAS EM {zip_path}: {e}", level="error")
        return 0


def estimate_total_lines_alternative(files_dir: str, encoding: str = 'latin1') -> int:
    """
    Estima o número total de registros lendo e contando as linhas dos arquivos. (Método lento)
    """
    print_log("CALCULANDO TOTAL DE REGISTROS (MÉTODO ALTERNATIVO)...", level="task")

    if not os.path.exists(files_dir):
        raise FileNotFoundError(f"DIRETÓRIO NÃO ENCONTRADO: {files_dir}")

    zip_files = [os.path.join(files_dir, f) for f in os.listdir(files_dir) if f.lower().endswith('.zip')]

    groups = defaultdict(list)
    for path in zip_files:
        base = os.path.basename(path).lower()
        prefix = ''.join(c for c in base if not c.isdigit()).replace('.zip', '').rstrip('-_')
        groups[prefix].append(path)

    total_lines = 0
    for group_name, paths in groups.items():
        paths.sort()
        n = len(paths)
        if n <= 3:
            group_line_count = sum(_count_lines(path, encoding) for path in paths)
        else:
            first = _count_lines(paths[0], encoding)
            second = _count_lines(paths[1], encoding)
            group_line_count = first + (second * (n - 1))
        total_lines += group_line_count

    print_log(f"TOTAL DE REGISTROS: {total_lines:,.0f}".replace(",", "."), level="info")
    return total_lines
