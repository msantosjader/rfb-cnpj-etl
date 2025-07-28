# db/postgres_builder.py

"""
Módulo para construção do banco de dados PostgreSQL.
"""

import psycopg2
from ..db.schema import SCHEMA
from ..utils.db_patch import apply_static_fixes
from ..utils.logger import print_log


class PostgresBuilder:
    """
    Classe para construção do banco de dados PostgreSQL.
    """

    def __init__(self, config):
        self.config = config
        self.conn = None

    def _connect(self):
        """Abre a conexão com o Postgres."""
        try:
            conn = psycopg2.connect(
                host=self.config["host"],
                port=self.config["port"],
                database=self.config["database"],
                user=self.config["user"],
                password=self.config["password"],
            )
            conn.set_client_encoding('WIN1252')
            return conn
        except psycopg2.Error as e:
            print_log(f"ERRO AO CONECTAR NO BANCO: {e}", level="error")
            raise

    def _create_database(self):
        try:
            temp_config = self.config.copy()
            temp_config["database"] = "postgres"
            conn = psycopg2.connect(**temp_config)
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

    def drop_tables(self):
        try:
            conn = self._connect()
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public';")
            for (table_name,) in cur.fetchall():
                cur.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE;')
            conn.close()
            print_log("TABELAS ANTERIORES REMOVIDAS", level="docs")
        except psycopg2.Error as e:
            print_log(f"ERRO AO EXCLUIR TABELAS: {e}", level="error")
            raise

    def create_tables(self):
        """
        Cria todas as tabelas. As chaves primárias definidas inline nas colunas
        (em tabelas menores) são criadas aqui. As PKs de tabelas maiores,
        definidas separadamente no SCHEMA, são adiadas.
        """
        try:
            print_log("CRIANDO TABELAS...", level="task")
            if self.conn is None: self.conn = self._connect()
            self.conn.autocommit = True
            cur = self.conn.cursor()

            for table_name, definition in SCHEMA.items():
                columns_sql = [
                    f'"{col_name}" {col_type}'
                    for col_name, col_type in definition['columns']
                ]
                columns_str = ", ".join(columns_sql)

                cur.execute(f'CREATE UNLOGGED TABLE IF NOT EXISTS public."{table_name}" ({columns_str});')

            print_log("TABELAS CRIADAS", level="success")
        except psycopg2.Error as e:
            print_log(f"ERRO AO CRIAR TABELAS: {e}", level="error")
            raise

    def _add_primary_keys(self):
        """
        Adiciona as chaves primárias APENAS para as tabelas que as definem
        separadamente no SCHEMA (ex: 'empresa', 'estabelecimento').
        """
        print_log("ADICIONANDO CHAVES PRIMÁRIAS (TABELAS GRANDES)...", level="task")
        if self.conn is None: self.conn = self._connect()
        self.conn.autocommit = True
        cur = self.conn.cursor()

        for table_name, definition in SCHEMA.items():
            pk_cols = []
            if 'primary_key' in definition:
                pk_cols = definition['primary_key']

            if pk_cols:
                pk_cols_str = ', '.join(f'"{col}"' for col in pk_cols)
                sql_command = f'ALTER TABLE public."{table_name}" ADD PRIMARY KEY ({pk_cols_str});'

                try:
                    print_log(f"  -> Adicionando PK em '{table_name}'...", level="docs")
                    cur.execute(sql_command)
                except psycopg2.Error as e:
                    # 42P16 = multiple_primary_keys, 42P07 = relation_already_exists
                    if e.pgcode in ('42P16', '42P07'):
                        print_log(f"     ℹ PK em '{table_name}' já existe, pulando.", level="docs")
                    else:
                        print_log(f"ERRO AO ADICIONAR PK em '{table_name}': {e}", level="error")
                        raise

        print_log("CHAVES PRIMÁRIAS (TABELAS GRANDES) ADICIONADAS", level="success")

    def patch_data(self):
        """
        Aplica correções estáticas nos dados e, em seguida, adiciona as
        chaves primárias restantes (das tabelas grandes).
        """
        if self.conn is None: self.conn = self._connect()
        # A ordem original é restaurada: primeiro o patch, depois as PKs restantes.
        apply_static_fixes(self.conn, engine="postgres")
        self._add_primary_keys()

    def enable_foreign_keys(self):
        """Cria as chaves estrangeiras definidas no SCHEMA."""
        try:
            print_log("CRIANDO CHAVES ESTRANGEIRAS...", level="task")
            if self.conn is None: self.conn = self._connect()
            self.conn.autocommit = True
            cur = self.conn.cursor()

            # 1. Coleta todas as definições de FKs para obter um total
            all_fks = []
            for table_name, definition in SCHEMA.items():
                if 'foreign_keys' in definition:
                    for i, fk_def in enumerate(definition['foreign_keys'], start=1):
                        all_fks.append((table_name, fk_def, i))

            # 2. Itera sobre a lista de FKs com um contador global
            total = len(all_fks)
            width = len(str(total))
            for i, (table_name, fk, fk_index) in enumerate(all_fks, start=1):
                constraint_name = f"fk_{table_name}_{fk_index}"
                try:
                    fk_columns = ', '.join(f'"{col}"' for col in fk['columns'])
                    ref_table_and_cols = fk['references']
                    ref_table = ref_table_and_cols.split('(')[0].strip()
                    ref_cols_str = ref_table_and_cols.split('(')[1].replace(')', '')
                    ref_cols = ', '.join(f'"{c.strip()}"' for c in ref_cols_str.split(','))

                    fk_sql = (f'ALTER TABLE public."{table_name}" '
                              f'ADD CONSTRAINT "{constraint_name}" '
                              f'FOREIGN KEY ({fk_columns}) '
                              f'REFERENCES public."{ref_table}"({ref_cols});')

                    cur.execute(fk_sql)
                    print_log(f"[{i:0{width}}/{total}] FK CRIADA: {constraint_name} em '{table_name}'", level="docs")

                except psycopg2.Error as e:
                    # 42710 = duplicate_object (constraint já existe)
                    if e.pgcode == '42710':
                        print_log(f"[{i:0{width}}/{total}] FK JÁ EXISTE: {constraint_name}, pulando.", level="docs")
                    else:
                        print_log(f"[{i:0{width}}/{total}] ERRO AO ADICIONAR FK '{constraint_name}': {e}",
                                  level="error")

            print_log("CHAVES ESTRANGEIRAS CRIADAS", level="success")
        except psycopg2.Error as e:
            print_log(f"ERRO AO CRIAR FKs: {e}", level="error")
            raise

    def create_indexes(self):
        """Cria os índices definidos no SCHEMA."""
        try:
            print_log("CRIANDO ÍNDICES...", level="task")
            if self.conn is None: self.conn = self._connect()
            self.conn.autocommit = True
            cur = self.conn.cursor()

            all_indexes = []
            for table_name, definition in SCHEMA.items():
                if 'indexes' in definition:
                    for index in definition['indexes']:
                        all_indexes.append((table_name, index))

            total = len(all_indexes)
            width = len(str(total))
            for i, (table_name, index) in enumerate(all_indexes, start=1):
                index_name = "desconhecido"
                try:
                    index_name = index['name']
                    index_cols = ', '.join(f'"{col}"' for col in index['columns'])
                    stmt = f'CREATE INDEX IF NOT EXISTS "{index_name}" ON public."{table_name}" ({index_cols});'
                    cur.execute(stmt)
                    print_log(f"[{i:0{width}}/{total}] ÍNDICE CRIADO: {index_name}", level="docs")
                except (psycopg2.Error, KeyError) as e:
                    print_log(f"Erro ao criar índice {index_name}: {e}", level="error")

            print_log("TODOS OS ÍNDICES FORAM CRIADOS", level="success")
        except psycopg2.Error as e:
            print_log(f"ERRO AO CRIAR ÍNDICES: {e}", level="error")
            raise

    def initialize_schema(self) -> None:
        """Executa o fluxo completo de criação do schema."""
        try:
            self._create_database()
            self.conn = self._connect()
            self.drop_tables()
            self.create_tables()
        finally:
            if self.conn:
                self.conn.close()
                self.conn = None
