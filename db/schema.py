# database/schema.py

"""
Define o schema completo do banco de dados da base CNPJ, incluindo o mapeamento
dos arquivos de origem para as tabelas de destino.

Esta estrutura consolidada é a "Fonte da Verdade" e facilita a criação
programática de tabelas, índices, constraints e a lógica de carga de dados.
"""

SCHEMA = {
    'cnae': {
        'source_file_stem': 'Cnaes',
        'columns': [
            ('cod_cnae', 'VARCHAR(7) PRIMARY KEY'),
            ('nome_cnae', 'VARCHAR(200) NOT NULL')
        ]
    },
    'motivo': {
        'source_file_stem': 'Motivos',
        'columns': [
            ('cod_motivo', 'VARCHAR(2) PRIMARY KEY'),
            ('nome_motivo', 'VARCHAR(100) NOT NULL')
        ]
    },
    'municipio': {
        'source_file_stem': 'Municipios',
        'columns': [
            ('cod_municipio', 'VARCHAR(4) PRIMARY KEY'),
            ('nome_municipio', 'VARCHAR(60) NOT NULL')
        ]
    },
    'natureza_juridica': {
        'source_file_stem': 'Naturezas',
        'columns': [
            ('cod_natureza', 'VARCHAR(4) PRIMARY KEY'),
            ('nome_natureza', 'VARCHAR(200) NOT NULL')
        ]
    },
    'pais': {
        'source_file_stem': 'Paises',
        'columns': [
            ('cod_pais', 'VARCHAR(3) PRIMARY KEY'),
            ('nome_pais', 'VARCHAR(60) NOT NULL')
        ]
    },
    'qualificacao_socio': {
        'source_file_stem': 'Qualificacoes',
        'columns': [
            ('cod_qualificacao', 'VARCHAR(2) PRIMARY KEY'),
            ('nome_qualificacao', 'VARCHAR(200) NOT NULL')
        ]
    },
    'empresa': {
        'source_file_stem': 'Empresas',
        'columns': [
            ('cnpj_basico', 'VARCHAR(8)'),
            ('razao_social', 'VARCHAR(200)'),
            ('cod_natureza_juridica', 'VARCHAR(4) NOT NULL'),
            ('cod_qualificacao_responsavel', 'VARCHAR(2) NOT NULL'),
            ('capital_social', 'NUMERIC(16,2) NOT NULL'),
            ('cod_porte', 'VARCHAR(2)'),
            ('ente_federativo_responsavel', 'VARCHAR(100)')
        ],
        'primary_key': ['cnpj_basico'],
        'foreign_keys': [
            {'columns': ['cod_natureza_juridica'], 'references': 'natureza_juridica(cod_natureza)'},
            {'columns': ['cod_qualificacao_responsavel'], 'references': 'qualificacao_socio(cod_qualificacao)'}
        ],
        'indexes': [
            {'name': 'idx_empresa_cnpj', 'columns': ['cnpj_basico']},
            {'name': 'idx_empresa_razao_social', 'columns': ['razao_social']},
            {'name': 'idx_empresa_natureza', 'columns': ['cod_natureza_juridica']},
            {'name': 'idx_empresa_porte', 'columns': ['cod_porte']}
        ]
    },
    'estabelecimento': {
        'source_file_stem': 'Estabelecimentos',
        'columns': [
            ('cnpj_basico', 'VARCHAR(8) NOT NULL'),
            ('cnpj_ordem', 'VARCHAR(4) NOT NULL'),
            ('cnpj_dv', 'VARCHAR(2) NOT NULL'),
            ('matriz_filial', 'VARCHAR(1) NOT NULL'),
            ('nome_fantasia', 'VARCHAR(60)'),
            ('cod_situacao_cadastral', 'VARCHAR(2) NOT NULL'),
            ('data_situacao_cadastral', 'DATE'),
            ('cod_motivo_situacao_cadastral', 'VARCHAR(2) NOT NULL'),
            ('nome_cidade_exterior', 'VARCHAR(60)'),
            ('cod_pais', 'VARCHAR(3)'),
            ('data_inicio_atividade', 'DATE NOT NULL'),
            ('cod_cnae_principal', 'VARCHAR(7) NOT NULL'),
            ('cod_cnae_secundario', 'TEXT'),
            ('tipo_logradouro', 'VARCHAR(20)'),
            ('logradouro', 'VARCHAR(60)'),
            ('numero', 'VARCHAR(6)'),
            ('complemento', 'VARCHAR(200)'),
            ('bairro', 'VARCHAR(60)'),
            ('cep', 'VARCHAR(8)'),
            ('uf', 'VARCHAR(2) NOT NULL'),
            ('cod_municipio', 'VARCHAR(4)'),
            ('ddd_telefone_1', 'VARCHAR(4)'),
            ('telefone_1', 'VARCHAR(10)'),
            ('ddd_telefone_2', 'VARCHAR(4)'),
            ('telefone_2', 'VARCHAR(10)'),
            ('ddd_fax', 'VARCHAR(4)'),
            ('fax', 'VARCHAR(10)'),
            ('email', 'TEXT'),
            ('situacao_especial', 'VARCHAR(100)'),
            ('data_situacao_especial', 'DATE')
        ],
        'primary_key': ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv'],
        'foreign_keys': [
            {'columns': ['cnpj_basico'], 'references': 'empresa(cnpj_basico)'},
            {'columns': ['cod_cnae_principal'], 'references': 'cnae(cod_cnae)'},
            {'columns': ['cod_municipio'], 'references': 'municipio(cod_municipio)'},
            {'columns': ['cod_pais'], 'references': 'pais(cod_pais)'},
            {'columns': ['cod_motivo_situacao_cadastral'], 'references': 'motivo(cod_motivo)'}
        ],
        'indexes': [
            {'name': 'idx_estab_empresa', 'columns': ['cnpj_basico']},
            {'name': 'idx_estab_nome_fantasia', 'columns': ['nome_fantasia']},
            {'name': 'idx_estab_cnae_principal', 'columns': ['cod_cnae_principal']},
            {'name': 'idx_estab_data_inicio', 'columns': ['data_inicio_atividade']},
            {'name': 'idx_estab_data_situacao', 'columns': ['data_situacao_cadastral']},
            {'name': 'idx_estab_municipio', 'columns': ['cod_municipio']},
            {'name': 'idx_estab_uf_municipio', 'columns': ['uf', 'cod_municipio']},
            {'name': 'idx_estab_situacao', 'columns': ['cod_situacao_cadastral']}
        ]
    },
    'simples': {
        'source_file_stem': 'Simples',
        'columns': [
            ('cnpj_basico', 'VARCHAR(8)'),
            ('opcao_simples', 'VARCHAR(1)'),
            ('data_opcao_simples', 'DATE'),
            ('data_exclusao_simples', 'DATE'),
            ('opcao_mei', 'VARCHAR(1)'),
            ('data_opcao_mei', 'DATE'),
            ('data_exclusao_mei', 'DATE')
        ],
        'foreign_keys': [
            {'columns': ['cnpj_basico'], 'references': 'empresa(cnpj_basico)'}
        ],
        'indexes': [
            {'name': 'idx_simples_empresa', 'columns': ['cnpj_basico']}
        ]
    },
    'socio': {
        'source_file_stem': 'Socios',
        'columns': [
            ('cnpj_basico', 'VARCHAR(8) NOT NULL'),
            ('identificador_socio', 'VARCHAR(1) NOT NULL'),
            ('nome_socio', 'VARCHAR(200)'),
            ('cnpj_cpf_socio', 'VARCHAR(14)'),
            ('cod_qualificacao_socio', 'VARCHAR(2) NOT NULL'),
            ('data_entrada_sociedade', 'DATE NOT NULL'),
            ('cod_pais', 'VARCHAR(3)'),
            ('cpf_representante_legal', 'VARCHAR(11)'),
            ('nome_representante_legal', 'VARCHAR(100)'),
            ('cod_qualificacao_representante_legal', 'VARCHAR(2)'),
            ('cod_faixa_etaria', 'VARCHAR(1) NOT NULL')
        ],
        'foreign_keys': [
            {'columns': ['cnpj_basico'], 'references': 'empresa(cnpj_basico)'},
            {'columns': ['cod_pais'], 'references': 'pais(cod_pais)'},
            {'columns': ['cod_qualificacao_socio'], 'references': 'qualificacao_socio(cod_qualificacao)'},
            {'columns': ['cod_qualificacao_representante_legal'], 'references': 'qualificacao_socio(cod_qualificacao)'}
        ],
        'indexes': [
            {'name': 'idx_socio_empresa', 'columns': ['cnpj_basico']},
            {'name': 'idx_socio_cpf_cnpj', 'columns': ['cnpj_cpf_socio']},
            {'name': 'idx_socio_nome', 'columns': ['nome_socio']}
        ]
    },

    'estabelecimento_cnae_sec': {
        'source_file_stem': 'Estabelecimentos',
        'columns': [
            ('cnpj_basico', 'VARCHAR(8) NOT NULL'),
            ('cnpj_ordem', 'VARCHAR(4) NOT NULL'),
            ('cnpj_dv', 'VARCHAR(2) NOT NULL'),
            ('cod_cnae', 'VARCHAR(7) NOT NULL')
        ],
        'foreign_keys': [
            {'columns': ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv'],
             'references': 'estabelecimento(cnpj_basico, cnpj_ordem, cnpj_dv)'},
            {'columns': ['cod_cnae'], 'references': 'cnae(cod_cnae)'}
        ],
        'indexes': [
            {'name': 'idx_cnae_sec_estab', 'columns': ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv']}
        ]
    }
}
