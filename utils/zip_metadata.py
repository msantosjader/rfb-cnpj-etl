# zip_metadata.py

"""
Módulo para lidar com metadados de arquivos ZIP.
"""

import io
import os
import zipfile
from collections import defaultdict
from cnpj_data.cnpj_public_data import CNPJDataScraper
from .logger import print_log


def validate_zip_files(month_year: str, files_dir: str):
    """
    Valida os arquivos baixados.

    :params:
        month_year: mês e ano a ser validado. Se None, usa o mês mais recente.
        files_dir: diretório onde os arquivos estão localizados. Se None, usa o diretório padrão.
    :return: True se todos os arquivos estão presentes e têm o tamanho correto.
    """

    # mensagens de execução
    print_log(f"VALIDAÇÃO DOS ARQUIVOS ZIP...", level="task")
    print_log(f"PERÍODO: {month_year}", level="info")
    print_log(f"LOCAL: {files_dir}", level="info")

    # verifica se o diretório existe
    if not os.path.exists(files_dir):
        raise FileNotFoundError(f"DIRETÓRIO NÃO ENCONTRADO: {files_dir}")

    # obtém os nomes e tamanhos dos arquivos zip locais
    local_files = {
        fname: os.path.getsize(os.path.join(files_dir, fname))
        for fname in os.listdir(files_dir)
        if fname.lower().endswith('.zip')
    }

    print_log(f"{len(local_files)} ARQUIVOS NA PASTA LOCAL", level="folder")

    data = CNPJDataScraper()  # cria uma instância da classe CNPJDataScraper
    # obtém os metadados dos arquivos remotos
    remote_metadata = data.get_metadata(month_year)
    remote_files = {
        info["filename"]: info["file_size"]
        for info in remote_metadata.values()
    }

    # se os arquivos locais e remotos são iguais, retorna True
    if local_files == remote_files:
        print_log(f"ARQUIVOS VALIDADOS", level="success")
        return True

    # se não, imprime os arquivos ausentes, excedentes e/ou com tamanhos diferentes
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


def _count_lines(zip_path: str, encoding: str = 'latin1') -> int:
    """
    Conta as linhas do único arquivo dentro do ZIP e retorna o número de linhas.

    :params:
        zip_path: caminho para o arquivo ZIP.
        encoding: codificação a ser usada para decodificar o arquivo CSV.
    :return: Número de linhas no arquivo, ou 0 em caso de erro.
    """
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            entry = z.infolist()[0]

            with z.open(entry, 'r') as bin_f, \
                 io.TextIOWrapper(bin_f, encoding=encoding, errors='ignore') as txt_f:
                return sum(1 for _ in txt_f)

    except Exception as e:
        print_log(f"❌ ERRO AO CONTAR LINHAS EM {zip_path}: {e}")
        return 0


def estimate_total_lines(files_dir: str, encoding: str = 'latin1') -> int:
    """
    Percorre os arquivos ZIP no diretório, estima o número total de registros (linhas de CSVs),
    agrupando arquivos com nomes semelhantes e aplicando amostragem para otimização.

    :params:
        files_dir: diretório onde os arquivos ZIP estão localizados.
        encoding: codificação a ser usada para decodificar o arquivo CSV.
    :return: Estimativa do número total de registros (linhas de CSVs).
    """
    print_log("CALCULANDO TOTAL DE REGISTROS...", level="task")

    if not os.path.exists(files_dir):
        raise FileNotFoundError(f"DIRETÓRIO NÃO ENCONTRADO: {files_dir}")

    zip_files = [
        os.path.join(files_dir, f)
        for f in os.listdir(files_dir)
        if f.lower().endswith('.zip')
    ]

    groups = defaultdict(list)
    for path in zip_files:
        base = os.path.basename(path).lower()
        prefix = ''.join(c for c in base if not c.isdigit()).replace('.zip', '').rstrip('-_')
        groups[prefix].append(path)

    total_lines = 0
    # total_groups = len(groups)

    for i, (group_name, paths) in enumerate(groups.items(), start=1):
        paths.sort()
        n = len(paths)
        # label = os.path.splitext(os.path.basename(paths[0]))[0].upper() if n <= 3 else group_name.upper()

        if n <= 3:
            group_line_count = 0
            for path in paths:
                group_line_count += _count_lines(path, encoding)
        else:
            first = _count_lines(paths[0], encoding)
            second = _count_lines(paths[1], encoding)
            group_line_count = first + (second * (n - 1))

        total_lines += group_line_count
#         print_log(
#             f"{i:>2}/{total_groups} {label:<17} → {group_line_count:>12,}".replace(",", "."),
#             level="debug"
#         )

    label = "TOTAL DE REGISTROS"
    # print_log(f"{label:<23} → {total_lines:>12,}".replace(",", "."), level="info")
    print_log(f"{label}: {total_lines:,.0f}".replace(",", "."), level="info")

    return total_lines