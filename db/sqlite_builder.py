# sqlite_builder.py

"""
Módulo para construção do banco de dados SQLite.
"""

import os
import sqlite3
from typing import Dict, Any, Optional
from config import SQLITE_DB_PATH
from db.schema import SCHEMA
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
        self.schema: Dict[str, Any] = SCHEMA
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

    def _close_connection(self):
        if self.conn:
            try:
                self.conn.close()
            except sqlite3.Error:
                pass
            self.conn = None

    def drop_database(self) -> None:
        """
        Fecha conexão pendente e exclui o arquivo .db, se existir.
        """
        try:
            self._close_connection()
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
        if self.conn is None: self.conn = self._connect()
        try:
            print_log("CRIANDO TABELAS...", level="task")
            cur = self.conn.cursor()
            for table_name, spec in self.schema.items():
                cur.execute(f'DROP TABLE IF EXISTS "{table_name}"')
                columns_defs = [f'"{col[0]}" {col[1]}' for col in spec['columns']]
                if 'primary_key' in spec:
                    pk_cols = ', '.join([f'"{col}"' for col in spec['primary_key']])
                    columns_defs.append(f'PRIMARY KEY ({pk_cols})')
                if 'foreign_keys' in spec:
                    for fk in spec['foreign_keys']:
                        fk_cols = ', '.join([f'"{col}"' for col in fk['columns']])
                        columns_defs.append(f"FOREIGN KEY ({fk_cols}) REFERENCES {fk['references']}")
                ddl = f'CREATE TABLE "{table_name}" (\n    ' + ',\n    '.join(columns_defs) + '\n);'
                cur.execute(ddl)
            self.conn.commit()
        except sqlite3.Error:
            raise
        finally:
            self._close_connection()

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
        if self.conn is None: self.conn = self._connect()
        try:
            print_log("ATIVANDO CHAVES ESTRANGEIRAS...", level="task")

            self.conn.execute("PRAGMA foreign_keys = ON;")
            self.conn.commit()

            print_log("CHAVES ESTRANGEIRAS ATIVADAS", level="success")
        except sqlite3.Error as e:
            print_log(f"ERRO AO ATIVAR CHAVES ESTRANGEIRAS: {e}", level="error")
            raise
        finally:
            self._close_connection()

    def create_indexes(self) -> None:
        """
        Cria índices recomendados para melhorar desempenho de consultas.
        """
        if self.conn is None: self.conn = self._connect()
        try:
            print_log("CRIANDO ÍNDICES...", level="task")

            cur = self.conn.cursor()

            # PRAGMAs para acelerar o trabalho
            cur.execute("PRAGMA journal_mode=MEMORY;")
            cur.execute("PRAGMA synchronous=OFF;")
            cur.execute("PRAGMA foreign_keys = OFF;")

            total_indexes = sum(len(spec.get('indexes', [])) for spec in self.schema.values())
            indexes_done = 0

            for table_name, spec in self.schema.items():
                if 'indexes' in spec:
                    for index in spec['indexes']:
                        index_name = index['name']
                        index_cols = ', '.join(index['columns'])

                        print_log(
                            f" {indexes_done + 1} de {total_indexes} | CRIANDO ÍNDICE '{index_name}' NA TABELA '{table_name}' (Colunas: {index_cols})... ",
                            level="task")

                        stmt = f"CREATE INDEX IF NOT EXISTS \"{index_name}\" ON \"{table_name}\" ({', '.join(f'\"{col}\"' for col in index['columns'])});"
                        cur.execute(stmt)

                        indexes_done += 1

            self.conn.commit()
            print_log("ÍNDICES CRIADOS", level="success")

        except sqlite3.Error as e:
            print_log(f"ERRO AO CRIAR ÍNDICES: {e}", level="error")
            raise

        finally:
            self._close_connection()

    def initialize_schema(self) -> None:
        """
        Fluxo de inicialização do script_sql:
          1) drop_database
          2) create_tables
        """
        self.drop_database()
        self.create_tables()
