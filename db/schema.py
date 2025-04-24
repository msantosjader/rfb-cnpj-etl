
# tabelas
tables = {
    "cnaes": {
        "nome_tabela": "cnae",
        "colunas": {
            "cod_cnae": "varchar(7) NOT NULL",
            "nome_cnae": "varchar(200) NOT NULL"
        }
    },
    "empresas": {
        "nome_tabela": "empresa",
        "colunas": {
            "cnpj_basico": "varchar(14) NOT NULL",
            "razao_social": "varchar(200)",
            "cod_natureza_juridica": "varchar(4) NOT NULL",
            "cod_qualificacao_responsavel": "varchar(4) NOT NULL",
            "capital_social": "numeric(16,2) NOT NULL",
            "cod_porte": "varchar(2)",
            "ente_federativo_responsavel": "varchar(100)",
        }
    },
    "estabelecimentos": {
        "nome_tabela": "estabelecimento",
        "colunas": {
            "cnpj_basico": "varchar(14) NOT NULL",
            "cnpj_ordem": "varchar(4) NOT NULL",
            "cnpj_dv": "varchar(2) NOT NULL",
            "matriz_filial": "varchar(1) NOT NULL",
            "nome_fantasia": "varchar(60)",
            "cod_situacao_cadastral": "varchar(2) NOT NULL",
            "data_situacao_cadastral": "date",
            "cod_motivo_situacao_cadastral": "varchar(2) NOT NULL",
            "nome_cidade_exterior": "varchar(60)",
            "cod_pais": "varchar(3)",
            "data_inicio_atividade": "date NOT NULL",
            "cod_cnae_principal": "varchar(7) NOT NULL",
            "cod_cnae_secundario": "varchar",
            "tipo_logradouro": "varchar(20)",
            "logradouro": "varchar(60)",
            "numero": "varchar(6)",
            "complemento": "varchar(200)",
            "bairro": "varchar(60)",
            "cep": "varchar(8)",
            "uf": "varchar(2) NOT NULL",
            "cod_municipio": "varchar(4)",
            "ddd_telefone_1": "varchar(4)",
            "telefone_1": "varchar(10)",
            "ddd_telefone_2": "varchar(4)",
            "telefone_2": "varchar(10)",
            "ddd_fax": "varchar(4)",
            "fax": "varchar(10)",
            "email": "varchar",
            "situacao_especial": "varchar(100)",
            "data_situacao_especial": "date"
        }
    },
    "motivos":{
        "nome_tabela": "motivo",
        "colunas": {
            "cod_motivo": "varchar(2) NOT NULL",
            "nome_motivo": "varchar(100) NOT NULL"
        }
    },
    "municipios": {
        "nome_tabela": "municipio",
        "colunas": {
            "cod_municipio": "varchar(4) NOT NULL",
            "nome_municipio": "varchar(60) NOT NULL"
        }
    },
    "naturezas": {
        "nome_tabela": "natureza_juridica",
        "colunas": {
            "cod_natureza": "varchar(4) NOT NULL",
            "nome_natureza": "varchar(200) NOT NULL"
        }
    },
    "paises": {
        "nome_tabela": "pais",
        "colunas": {
            "cod_pais": "varchar(3) NOT NULL",
            "nome_pais": "varchar(60) NOT NULL"
        }
    },
    "qualificacoes": {
        "nome_tabela": "qualificacao_socio",
        "colunas": {
            "cod_qualificacao": "varchar(2) NOT NULL",
            "nome_qualificacao": "varchar(200) NOT NULL"
        }
    },
    "simples": {
        "nome_tabela": "simples",
        "colunas": {
            "cnpj_basico": "varchar(14) NOT NULL",
            "opcao_simples": "varchar(1)",
            "data_opcao_simples": "date",
            "data_exclusao_simples": "date",
            "opcao_mei": "varchar(1)",
            "data_opcao_mei": "date",
            "data_exclusao_mei": "date",
        }
    },
    "socios": {
        "nome_tabela": "socio",
        "colunas": {
            "cnpj_basico": "varchar(14) NOT NULL",
            "identificador_socio": "varchar(1) NOT NULL",
            "nome_socio": "varchar(200)",
            "cnpj_cpf_socio": "varchar(14)",
            "cod_qualificacao_socio": "varchar(2) NOT NULL",
            "data_entrada_sociedade": "date NOT NULL",
            "cod_pais": "varchar(3)",
            "cpf_representante_legal": "varchar(11)",
            "nome_representante_legal": "varchar(100)",
            "cod_qualificacao_representante_legal": "varchar(2)",
            "cod_faixa_etaria": "varchar(1) NOT NULL",
        }
    },
}

# chaves primárias e estrangeiras
keys = {
    "primary": {
        "cnae": ["cod_cnae"],
        "motivo": ["cod_motivo"],
        "municipio": ["cod_municipio"],
        "natureza_juridica": ["cod_natureza"],
        "pais": ["cod_pais"],
        "qualificacao_socio": ["cod_qualificacao"]
    },
    "foreign": {
        "empresa": [
            "FOREIGN KEY (cod_natureza_juridica) REFERENCES natureza_juridica(cod_natureza)",
            "FOREIGN KEY (cod_qualificacao_responsavel) REFERENCES qualificacao_socio(cod_qualificacao)"
        ],
        "estabelecimento": [
            "FOREIGN KEY (cnpj_basico) REFERENCES empresa(cnpj_basico)",
            "FOREIGN KEY (cod_cnae_principal) REFERENCES cnae(cod_cnae)",
            "FOREIGN KEY (cod_municipio) REFERENCES municipio(cod_municipio)",
            "FOREIGN KEY (cod_pais) REFERENCES pais(cod_pais)",
            "FOREIGN KEY (cod_motivo_situacao_cadastral) REFERENCES motivo(cod_motivo)"
        ],
        "simples": [
            "FOREIGN KEY (cnpj_basico) REFERENCES empresa(cnpj_basico)"
        ],
        "socio": [
            "FOREIGN KEY (cnpj_basico) REFERENCES empresa(cnpj_basico)",
            "FOREIGN KEY (cod_pais) REFERENCES pais(cod_pais)",
            "FOREIGN KEY (cod_qualificacao_socio) REFERENCES qualificacao_socio(cod_qualificacao)",
            "FOREIGN KEY (cod_qualificacao_representante_legal) REFERENCES qualificacao_socio(cod_qualificacao)"
        ]
    }
}

# índices para acelerar consultas
indexes = [
    # empresa
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_empresa_cnpj ON empresa(cnpj_basico);",
    "CREATE INDEX IF NOT EXISTS idx_empresa_razao_social ON empresa(razao_social);",
    "CREATE INDEX IF NOT EXISTS idx_empresa_natureza ON empresa(cod_natureza_juridica);",
    "CREATE INDEX IF NOT EXISTS idx_empresa_qualificacao ON empresa(cod_qualificacao_responsavel);",
    "CREATE INDEX IF NOT EXISTS idx_empresa_porte ON empresa(cod_porte);",

    # estabelecimento
    "CREATE INDEX IF NOT EXISTS idx_estab_empresa ON estabelecimento(cnpj_basico);",
    "CREATE INDEX IF NOT EXISTS idx_estab_nome_fantasia ON estabelecimento(nome_fantasia);",
    "CREATE INDEX IF NOT EXISTS idx_estab_cnae_principal ON estabelecimento(cod_cnae_principal);",
    "CREATE INDEX IF NOT EXISTS idx_estab_municipio ON estabelecimento(cod_municipio);",
    "CREATE INDEX IF NOT EXISTS idx_estab_data_inicio ON estabelecimento(data_inicio_atividade);",
    "CREATE INDEX IF NOT EXISTS idx_estab_data_situacao ON estabelecimento(data_situacao_cadastral);",
    "CREATE INDEX IF NOT EXISTS idx_estab_uf_municipio ON estabelecimento(uf, cod_municipio);",
    "CREATE INDEX IF NOT EXISTS idx_estab_situacao ON estabelecimento(cod_situacao_cadastral);",

    # simples
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_simples_empresa ON simples(cnpj_basico);",

    # socio
    "CREATE INDEX IF NOT EXISTS idx_socio_empresa ON socio(cnpj_basico);",
    "CREATE INDEX IF NOT EXISTS idx_socio_qualificacao ON socio(cod_qualificacao_socio);",
    "CREATE INDEX IF NOT EXISTS idx_socio_cpf_cnpj ON socio(cnpj_cpf_socio);",
    "CREATE INDEX IF NOT EXISTS idx_socio_nome ON socio(nome_socio);"
]

# chaves primárias não criadas para não sacrificar performance:
# "primary": {
#     "empresa": ["cnpj_basico"],
#     "estabelecimento": ["cnpj_basico", "cnpj_ordem", "cnpj_dv"],
#     "simples": ["cnpj_basico"]
# },