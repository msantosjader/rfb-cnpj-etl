# utils/db_patch.py

"""
Aplica correções estáticas na base de dados.
"""

from ..config import DEFAULT_ENGINE
from ..utils.logger import print_log


def apply_static_fixes(conn, engine: str = DEFAULT_ENGINE):
    """
    Aplica correções estáticas na base de dados.

    :params:
        conn: conexão com o banco de dados.
        engine: engine do banco de dados.
    """
    try:
        print_log("APLICANDO CORREÇÕES NA BASE DE DADOS...", level="task")
        cur = conn.cursor()

        cur.execute("""
                    INSERT INTO qualificacao_socio (cod_qualificacao, nome_qualificacao)
                    VALUES ('36', 'Gerente-Delegado')
                    ON CONFLICT (cod_qualificacao) DO NOTHING;
                    """)

        cur.execute("""
                    INSERT INTO motivo (cod_motivo, nome_motivo)
                    VALUES ('32', 'DECURSO DE PRAZO DE INTERRUPCAO TEMPORARIA'),
                           ('81', 'SOLICITACAO DA ADMINISTRACAO TRIBUTARIA MUNICIPAL/ESTADUAL - SC'),
                           ('93', 'CNPJ - TITULAR BAIXADO')
                    ON CONFLICT (cod_motivo) DO NOTHING;
                    """)

        cur.execute("""
                    INSERT INTO pais (cod_pais, nome_pais)
                    VALUES ('008', 'ABU DHABI'),
                           ('009', 'DIRCE'),
                           ('015', 'ALAND, ILHAS'),
                           ('150', 'JERSEY'),
                           ('151', 'CANARIAS, ILHAS'),
                           ('200', 'CURACAO'),
                           ('321', 'GUERNSEY'),
                           ('359', 'MAN, ILHA DE'),
                           ('367', 'INGLATERRA'),
                           ('393', 'JERSEY'),
                           ('449', 'MACEDONIA (ANTIGA REP. IUGOSLAVA)'),
                           ('452', 'MADEIRA, ILHA DA'),
                           ('498', 'MOLDAVIA'),
                           ('678', 'SAO TOME E PRINCIPE'),
                           ('699', 'SAO MARTINHO, ILHA DE (PARTE HOLANDESA)'),
                           ('737', 'SERVIA'),
                           ('994', 'AZERBAIJAO')
                    ON CONFLICT (cod_pais) DO NOTHING;
                    """)

        if engine == "postgres":
            query_delete_duplicatas = """
                                      DELETE \
                                      FROM empresa
                                      WHERE ctid IN (SELECT ctid \
                                                     FROM (SELECT ctid, \
                                                                  ROW_NUMBER() OVER (PARTITION BY cnpj_basico ORDER BY CASE \
                                                                                                                           WHEN razao_social IS NOT NULL AND TRIM(razao_social) <> '' \
                                                                                                                               THEN 0 \
                                                                                                                           ELSE 1 END, ctid) as rn \
                                                           FROM empresa) t \
                                                     WHERE t.rn > 1); \
                                      """
            cur.execute(query_delete_duplicatas)

        else:  # sqlite
            query_delete_duplicatas_sqlite = """
                                             DELETE \
                                             FROM empresa
                                             WHERE rowid IN (SELECT rowid \
                                                             FROM (SELECT rowid, \
                                                                          ROW_NUMBER() OVER (PARTITION BY cnpj_basico ORDER BY CASE \
                                                                                                                                   WHEN razao_social IS NOT NULL AND TRIM(razao_social) <> '' \
                                                                                                                                       THEN 0 \
                                                                                                                                   ELSE 1 END, rowid) as rn \
                                                                   FROM empresa) t \
                                                             WHERE t.rn > 1); \
                                             """
            cur.execute(query_delete_duplicatas_sqlite)

        cur.execute("UPDATE estabelecimento SET cod_pais = NULL WHERE cod_pais = '0';")

        cur.execute("UPDATE empresa SET cod_porte = '00' WHERE cod_porte = '';")

        if engine == "postgres":
            cur.execute("""
                        UPDATE estabelecimento
                        SET cod_pais = LPAD(cod_pais, 3, '0')
                        WHERE cod_pais IS NOT NULL
                          AND LENGTH(TRIM(cod_pais)) = 2;
                        """)
        else:  # sqlite
            cur.execute("""
                        UPDATE estabelecimento
                        SET cod_pais = substr('000' || cod_pais, -3)
                        WHERE cod_pais IS NOT NULL
                          AND LENGTH(TRIM(cod_pais)) = 2;
                        """)

        cur.execute("""
                    DELETE
                    FROM simples
                    WHERE cnpj_basico IN (
                                          '24417449', '24539162', '30721933', '30728066',
                                          '30760363', '30847991', '30857441', '30886793', '30972017'
                        );
                    """)

        conn.commit()
        print_log("CORREÇÕES APLICADAS", level="success")

    except Exception as e:
        print_log(f"ERRO AO APLICAR CORREÇÕES: {e}", level="error")
        raise
