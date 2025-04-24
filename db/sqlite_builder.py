# sqlite_builder.py

"""
Módulo para construção do banco de dados SQLite.
"""

import os
import re
import sqlite3
from typing import Optional
from config import SQLITE_DB_PATH
from db.schema import indexes, tables, keys
from utils.db_patch import apply_static_fixes
from utils.logger import print_log


class SQLiteBuilder:
    """
    Classe para construção do banco de dados SQLite.

    :params:
        db_path: caminho para o banco de dados SQLite.
        tables: dicionário de tabelas e colunas.
        conn: conexão com o banco de dados.
    """
    def __init__(self, db_path: Optional[str] = SQLITE_DB_PATH):
        self.db_path = db_path
        self.tables = tables
        self.conn = None

    def _connect(self) -> sqlite3.Connection:
        """
        Abre a conexão com o SQLite e desabilita a checagem de FKs temporariamente.

        :returns: conexão SQLite pronta para uso
        :raises sqlite3.Error: em caso de falha na conexão
        """
        try:
            conn = sqlite3.connect(self.db_path)
            conn.execute("PRAGMA foreign_keys = OFF;")
            return conn
        except sqlite3.Error as e:
            print_log(f"ERRO AO CONECTAR NO BANCO: {e}", level="error")
            raise

    def drop_database(self) -> None:
        """
        Fecha conexão pendente e exclui o arquivo .db, se existir.
        """
        try:
            if self.conn:
                self.conn.close()
                self.conn = None
                print_log("CONEXÃO EXISTENTE FECHADA", level="info")

            if os.path.exists(self.db_path):
                os.remove(self.db_path)

        except Exception as e:
            print_log(f"ERRO AO RECRIAR O BANCO: {e}", level="error")
            raise

    def create_tables(self) -> None:
        """
        Cria todas as tabelas definidas em "tables", incluindo PKs e FKs.

        As FKs permanecem desativadas até a chamada posterior de "enable_foreign_keys()".
        """
        try:
            print_log("CRIANDO TABELAS...", level="task")

            if self.conn is None:
                self.conn = self._connect()

            cur = self.conn.cursor()
            for meta in self.tables.values():
                name = meta["nome_tabela"]

                # drop table, se existir
                cur.execute(f'DROP TABLE IF EXISTS "{name}"')

                # monta as colunas
                cols = [f"{col} {tipo}" for col, tipo in meta["colunas"].items()]

                # adiciona as chaves primárias
                pk_cols = keys["primary"].get(name)
                if pk_cols:
                    cols.append(f"PRIMARY KEY ({', '.join(pk_cols)})")

                # adiciona as chaves estrangeiras
                for fk in keys["foreign"].get(name, []):
                    cols.append(fk)

                # cria as tabelas
                ddl = (
                        f'CREATE TABLE "{name}" (\n    '
                        + ",\n    ".join(cols)
                        + "\n);"
                )
                cur.execute(ddl)

            self.conn.commit()
            print_log("TABELAS CRIADAS", level="success")
        except sqlite3.Error as e:
            print_log(f"ERRO AO CRIAR TABELAS: {e}", level="error")
            raise

    def patch_data(self):
        """
        Normaliza os dados de algumas tabelas, permitindo a criação das chaves estrangeiras.
        """
        if self.conn is None:
            self.conn = self._connect()
        apply_static_fixes(self.conn, engine="sqlite")


    def enable_foreign_keys(self) -> None:
        """
        Ativa a verificação de chaves estrangeiras no SQLite.
        """
        try:
            print_log("ATIVANDO CHAVES ESTRANGEIRAS...", level="task")

            if self.conn is None:
                self.conn = self._connect()
            self.conn.execute("PRAGMA foreign_keys = ON;")
            self.conn.commit()

            print_log("CHAVES ESTRANGEIRAS ATIVADAS", level="success")
        except sqlite3.Error as e:
            print_log(f"ERRO AO ATIVAR CHAVES ESTRANGEIRAS: {e}", level="error")
            raise
        finally:
            if self.conn:
                self.conn.close()
                self.conn = None


    def create_indexes(self) -> None:
        """
        Cria índices recomendados para melhorar desempenho de consultas.
        """
        try:
            print_log("CRIANDO ÍNDICES...", level="task")

            if self.conn is None:
                self.conn = self._connect()

            cur = self.conn.cursor()

            # PRAGMAs para acelerar o trabalho
            cur.execute("PRAGMA journal_mode=MEMORY;")
            cur.execute("PRAGMA synchronous=OFF;")
            cur.execute("PRAGMA temp_store=MEMORY;")
            cur.execute("PRAGMA cache_size=-128000;")
            cur.execute("PRAGMA locking_mode = EXCLUSIVE;")
            cur.execute("PRAGMA foreign_keys = OFF;")

            total_indexes = len(indexes)
            width = len(str(total_indexes))
            for i, stmt in enumerate(indexes, start=1):
                try:
                    cur.execute(stmt)
                    match = re.search(r"INDEX\s+IF\s+NOT\s+EXISTS\s+(\w+)\s+ON", stmt, re.IGNORECASE)
                    index_name = match.group(1) if match else "?"
                    print_log(f"[{i:0{width}}/{total_indexes}] ÍNDICE CRIADO: {index_name}", level="info")
                except sqlite3.Error as e:
                    print_log(f"ERRO NO ÍNDICE ({stmt}): {e}", level="error")

            self.conn.commit()
            print_log("ÍNDICES CRIADOS", level="success")

        except sqlite3.Error as e:
            print_log(f"ERRO AO CRIAR ÍNDICES: {e}", level="error")
            raise

        finally:
            if self.conn:
                self.conn.close()
                self.conn = None

    def initialize_schema(self) -> None:
        """
        Fluxo de inicialização do script_sql:
          1) drop_database
          2) create_tables
        """
        try:
            self.drop_database()
            self.create_tables()
        finally:
            if self.conn:
                self.conn.close()
                self.conn = None
