import argparse
from cnpj_public_data import CNPJDataScraper
from cnpj_downloader import CNPJDownloadManager
from logger_utils import print_log

def main():
    parser = argparse.ArgumentParser(
        description="Aplicação para consultar, baixar e carregar dados do CNPJ"
    )

    # cria um subparser para cada comando
    sub = parser.add_subparsers(dest="command", required=True)

    # consultas à RFB
    sub.add_parser("get-availables", help="Lista meses disponíveis")
    sub.add_parser("get-latest", help="Mês mais recente disponível")
    p_urls = sub.add_parser("get-urls", help="Exibe URLs de um mês")
    p_urls.add_argument("month", type=str, help="MM/AAAA")

    p_dl = sub.add_parser("download", help="Baixa ZIPs de um ou mais meses")
    p_dl.add_argument("month", nargs="?", help="Mês no formato MM/AAAA (ex: 03/2025)")
    p_dl.add_argument("--clean", action="store_true", help="Remove arquivos antigos")
    p_dl.add_argument("--workers", type=int, help="Número de downloads simultâneos")
    p_dl.add_argument("--download-dir", type=str, help="Diretório para salvar os arquivos")


    # parse os argumentos de linha de comando
    args = parser.parse_args()

    try:
        if args.command == "get-availables":
            data = CNPJDataScraper()
            print_log(data.get_availabes(), level="info", time=False)

        elif args.command == "get-latest":
            data = CNPJDataScraper()
            print_log(data.get_latest(), level="info", time=False)

        elif args.command == "get-urls":
            data = CNPJDataScraper()
            urls = data.get_urls(month_year=args.month)
            for info in urls.values():
                print_log(info["url"], level="web", time=False)

        elif args.command == "download":
            try:
                dm = CNPJDownloadManager(
                    month_year=args.month,
                    concurrents= args.workers,
                    clean=args.clean,
                    download_dir=args.download_dir,
                )
                dm.start_download_queue()
            except Exception as e:
                print_log(f"{e}. EXECUÇÃO INTERROMPIDA", level="error")  # mostra só a mensagem

    except ValueError as e:
        print_log(f"{e}", level="error", time=False)  # mostra só a mensagem

if __name__ == "__main__":
    main()