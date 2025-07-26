# cnpj_public_data.py

"""
Módulo para acessar os dados de CNPJ disponíveis no site da Receita Federal.
"""

import os
import re
import requests
from bs4 import BeautifulSoup
from typing import Dict, Tuple, Optional

from config import CNPJ_DATA_URL


class CNPJDataScraper:
    """
    Classe para acessar os dados de CNPJ disponíveis no site da Receita Federal.

    :params:
        cnpj_data_url: URL base para acesso aos dados.
        _session: sessão HTTP persistente.
    """

    def __init__(self):
        self.cnpj_data_url = CNPJ_DATA_URL  # url base para acesso aos dados
        self._session = requests.Session()  # cria uma sessão HTTP persistente

    @staticmethod
    def _is_valid_period(month_year: str) -> bool:
        """
        Verifica se uma string está no formato MM/AAAA.

        :param month_year: string no formato MM/AAAA
        :return: bool
        """
        match = re.match(r'^(\d{2})/(\d{4})$', month_year)  # regex para validar o formato MM/AAAA
        if not match:
            return False

        return True

    @staticmethod
    def _parse_month(month_year: str) -> Tuple[int, int]:
        """
        Converte uma string no formato MM/AAAA em um par (mês, ano).

        :param month_year: string no formato MM/AAAA
        :return: par (mês, ano)
        """
        year_month = month_year.rstrip('/')
        try:
            year, month = year_month.split('-')
            return int(year), int(month)
        except ValueError:
            return 0, 0

    def _available_months(self) -> Dict[str, str]:
        """
        Obtém os meses disponíveis para download.

        :return: Dicionário com os meses disponíveis.
        """
        resp = self._session.get(self.cnpj_data_url)  # faz uma requisição GET para a URL base
        resp.raise_for_status()  # verifica se a requisição foi bem-sucedida
        soup = BeautifulSoup(resp.text, 'html.parser')  # analisa o HTML da página

        # extrai os links para os meses disponíveis
        raw_hrefs = [
            tag['href']
            for tag in soup.find_all('a', href=True)
            if re.match(r'^\d{4}-\d{2}/$', tag['href'])
        ]

        # converte os links em um dicionário com os meses disponíveis
        month_years = {
            f"{href[5:7]}/{href[:4]}": href.rstrip('/')
            for href in raw_hrefs
        }

        # verifica se há meses disponíveis
        if not month_years:
            raise ValueError(f"NENHUM PERÍODO DISPONÍVEL NO SITE ({self.cnpj_data_url})")

        # ordena os meses disponíveis em ordem decrescente
        sorted_month_years = dict(
            sorted(month_years.items(),
                   key=lambda item: self._parse_month(item[1]),
                   reverse=True)
        )

        return sorted_month_years

    def get_availabes(self):
        """
        Obtém os meses disponíveis para download.

        :return: String com os meses disponíveis.
        """
        month_years = self._available_months()
        availables = ", ".join(month_years.keys())
        return availables

    def get_latest(self):
        """
        Obtém o último mês disponível para download.

        :return: String com o último mês disponível.
        """
        month_years = self._available_months()
        latest = next(iter(month_years.keys()))
        return latest

    def get_metadata(self, month_year: Optional[str] = None) -> Dict[str, Dict[str, str]]:
        """
        Obtém as URLs dos arquivos de CNPJ disponíveis para um mês específico.

        :param month_year: string no formato MM/AAAA
        :return: Dicionário com as URLs dos arquivos de CNPJ disponíveis.
        """
        # se o mês não foi especificado, usa o mais recente
        if month_year is None:
            month_year = self.get_latest()

        # verifica se o mês é válido
        elif not self._is_valid_period(month_year):
            raise ValueError(f"{month_year} NÃO É UM FORMATO VÁLIDO (MM/AAAA)")

        # verifica se o mês está disponível
        month_years_map = self._available_months()
        if month_year not in month_years_map:
            raise ValueError(f"{month_year} NÃO ESTÁ DISPONÍVEL PARA DOWNLOAD")

        folder = month_years_map[month_year]  # obtém o período no formato AAAA-MM
        folder_url = f"{self.cnpj_data_url}{folder}/"  # cria a URL do mês (base + AAAA-MM)

        resp = self._session.get(folder_url)  # faz uma requisição GET para a URL do mês
        resp.raise_for_status()  # verifica se a requisição foi bem-sucedida

        # extrai as URLs dos arquivos de CNPJ disponíveis para o período
        soup = BeautifulSoup(resp.text, 'html.parser')
        hrefs = [
            tag['href']
            for tag in soup.find_all('a', href=True)
            if tag['href'].lower().endswith('.zip')
        ]

        # cria um dicionário com as URLs dos arquivos de CNPJ disponíveis
        result: Dict[str, Dict[str, str]] = {}
        for href in hrefs:
            filename = os.path.basename(href)
            file_url = f"{folder_url}{href}"
            key = os.path.join(folder, filename)

            resp = self._session.head(file_url, allow_redirects=True, timeout=10)
            resp.raise_for_status()
            cl = resp.headers.get("Content-Length")
            if cl is None:
                file_size = 0
            else:
                file_size = int(cl)

            result[key] = {
                "month_year": month_year,  # período (AAAA-MM)
                "filename": filename,  # nome do arquivo
                "file_url": file_url,  # url do arquivo
                "file_size": file_size  # tamanho do arquito (bytes)
            }

        # ordena os arquivos por nome
        result_sorted = dict(sorted(result.items(), key=lambda item: item[1]["filename"]))
        return result_sorted
