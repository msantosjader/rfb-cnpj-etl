# postgres_builder.py

"""
Módulo para construção do banco de dados PostgreSQL.
"""

import re
import psycopg2
from db.schema import indexes, tables, keys
from utils.db_patch import apply_static_fixes
from utils.logger import print_log


class PostgresBuilder:
    """
    Classe para construção do banco de dados PostgreSQL.

    :params:
        config: configurações do banco de dados.
        tables: dicionário de tabelas e colunas.
        conn: conexão com o banco de dados.
    """
    def __init__(self, config):
        self.config = config    # dados de conexão com o banco de dados
        self.tables = tables    # dicionário de tabelas e colunas
        self.conn = None


    def _connect(self):
        """
        Abre a conexão com o Postgres.

        :returns: conexão Postgres pronta para uso
        :raises psycopg2.Error: em caso de falha na conexão
        """
        try:
            conn = psycopg2.connect(
                host=self.config["host"],
                port=self.config["port"],
                database=self.config["database"],
                user=self.config["user"],
                password=self.config["password"],
            )
            conn.set_client_encoding('WIN1252')  # Agora cliente e servidor estão alinhados
            conn.autocommit = True
            return conn
        except psycopg2.Error as e:
            try:
                print_log(f"ERRO AO CONECTAR NO BANCO: {e}", level="error")
            except UnicodeDecodeError:
                print_log("ERRO AO CONECTAR NO BANCO: [mensagem contém caracteres inválidos]", level="error")
            raise


    def _create_database(self):
        """
        Cria o banco de dados se ele não existir.
        """
        try:
            temp_config = self.config.copy()
            temp_config["database"] = "postgres"

            conn = psycopg2.connect(
                host=temp_config["host"],
                port=temp_config["port"],
                database=temp_config["database"],
                user=temp_config["user"],
                password=temp_config["password"],
                options='-c client_encoding=LATIN1'
            )
            conn.autocommit = True
            cur = conn.cursor()

            db_name = self.config['database']
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (db_name,))
            exists = cur.fetchone()

            if not exists:
                cur.execute(f"CREATE DATABASE {db_name} ENCODING 'WIN1252' TEMPLATE template0;")
                print_log("BANCO CRIADO", level="success")

            conn.close()
        except psycopg2.Error as e:
            print_log(f"ERRO AO CRIAR BANCO: {e}", level="error")
            raise


    def drop_database(self):
        """
        Exclui o banco de dados.
        """
        try:
            conn = self._connect()
            conn.autocommit = True
            cur = conn.cursor()

            cur.execute("""
                SELECT tablename
                FROM pg_tables
                WHERE schemaname = 'public';
            """)

            fetched_tables  = cur.fetchall()

            for (table_name,) in fetched_tables :
                cur.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE;')

            conn.close()
        except psycopg2.Error as e:
            print_log(f"ERRO AO EXCLUIR TABELAS: {e}", level="error")
            raise


    def create_tables(self):
        """
        Cria todas as tabelas definidas em "tables".
        """
        try:
            print_log("CRIANDO TABELAS...", level="task")

            if self.conn is None:
                self.conn = self._connect()

            cur = self.conn.cursor()

            for schema_name, table_info in self.tables.items():
                table_name = table_info["nome_tabela"]
                columns = table_info["colunas"]

                # monta as colunas
                columns_sql = [f"{col} {col_type}" for col, col_type in columns.items()]

                # adiciona as chaves primárias
                pk_columns = keys.get("primary", {}).get(table_name)
                if pk_columns:
                    pk_str = ", ".join(pk_columns)
                    columns_sql.append(f"PRIMARY KEY ({pk_str})")

                # cria as tabelas
                columns_str = ", ".join(columns_sql)
                cur.execute(f'CREATE UNLOGGED TABLE IF NOT EXISTS "{table_name}" ({columns_str});')

            print_log("TABELAS CRIADAS", level="success")

        except psycopg2.Error as e:
            print_log(f"ERRO AO CRIAR TABELAS: {e}", level="error")
            raise


    def patch_data(self):
        """
        Normaliza os dados de algumas tabelas, permitindo a criação das chaves estrangeiras.
        """
        if self.conn is None:
            self.conn = self._connect()
        apply_static_fixes(self.conn, engine="postgres")


    def enable_foreign_keys(self):
        """
        Cria as chaves estrangeiras definidas em "keys".
        """
        try:
            print_log("CRIANDO CHAVES ESTRANGEIRAS...", level="task")

            if self.conn is None:
                self.conn = self._connect()

            cur = self.conn.cursor()

            for table, foreign_keys in keys.get("foreign", {}).items():
                for i, fk in enumerate(foreign_keys):
                    constraint_name = f"fk_{table}_{i + 1}"

                    # Verifica se a constraint já existe
                    cur.execute("""
                                SELECT 1
                                FROM information_schema.table_constraints
                                WHERE constraint_name = %s
                                  AND table_name = %s
                                  AND constraint_type = 'FOREIGN KEY';
                                """, (constraint_name, table))

                    if cur.fetchone():
                        continue

                    alter_sql = f'ALTER TABLE "{table}" ADD CONSTRAINT {constraint_name} {fk};'
                    cur.execute(alter_sql)

            self.conn.commit()
            print_log("CHAVES ESTRANGEIRAS CRIADAS", level="success")

        except psycopg2.Error as e:
            print_log(f"ERRO AO CRIAR FKs: {e}", level="error")
            raise


    def create_indexes(self):
        """
        Cria índices recomendados para melhorar desempenho de consultas.
        """
        try:
            print_log("CRIANDO ÍNDICES...", level="task")

            if self.conn is None:
                self.conn = self._connect()

            cur = self.conn.cursor()

            total_indexes = len(indexes)
            width = len(str(total_indexes))
            for i, stmt in enumerate(indexes, start=1):
                try:
                    cur.execute(stmt)
                    match = re.search(r"INDEX\s+IF\s+NOT\s+EXISTS\s+(\w+)\s+ON", stmt, re.IGNORECASE)
                    index_name = match.group(1) if match else "?"
                    print_log(f"[{i:0{width}}/{total_indexes}] ÍNDICE CRIADO: {index_name}", level="info")
                except psycopg2.Error as e:
                    print_log(f"Erro ao criar índice: {e}", level="error")

            print_log("TODOS OS ÍNDICES FORAM CRIADOS", level="success")

        except psycopg2.Error as e:
            print_log(f"ERRO AO CRIAR ÍNDICES: {e}", level="error")
            raise


    def initialize_schema(self) -> None:
        """
        Fluxo de inicialização do script_sql:
          1) cria banco se não existir
          2) drop_database
          3) create_tables
        """
        try:
            self._create_database()
            self.drop_database()
            self.create_tables()
        finally:
            if self.conn:
                self.conn.close()
                self.conn = None