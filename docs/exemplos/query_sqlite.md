# Exemplo de Consultas SQLite

Aqui está um exemplo de como consultar o banco de dados do projeto.

## Consulta Geral de Estabelecimentos

Esta consulta busca informações detalhadas de estabelecimentos ativos em Pernambuco, abertos em janeiro de 2022.
Inclui dados da empresa, endereço, quadro societário e informações do Simples Nacional.

```sql
SELECT
    (est.cnpj_basico || est.cnpj_ordem || est.cnpj_dv) AS "CNPJ",
    (e.razao_social) AS "Razão Social",
    (CASE est.matriz_filial WHEN '1' THEN 'MATRIZ' WHEN '2' THEN 'FILIAL' ELSE 'NÃO INFORMADO' END) AS "Matriz / Filial",
    (est.nome_fantasia) AS "Nome Fantasia",
    (CASE WHEN est.data_inicio_atividade > '0001-01-01' THEN strftime('%d/%m/%Y', est.data_inicio_atividade) ELSE '' END) AS "Data de Abertura",
    (CASE est.cod_situacao_cadastral WHEN '01' THEN '01 - NULA' WHEN '02' THEN '02 - ATIVA' WHEN '03' THEN '03 - SUSPENSA' WHEN '04' THEN '04 - INAPTA' WHEN '08' THEN '08 - BAIXADA' ELSE 'DESCONHECIDA' END) AS "Situação Cadastral",
    (CASE WHEN est.data_situacao_cadastral > '0001-01-01' THEN strftime('%d/%m/%Y', est.data_situacao_cadastral) ELSE '' END) AS "Data da Situação Cadastral",
    (est.cod_motivo_situacao_cadastral || ' - ' || COALESCE(mot.nome_motivo, 'MOTIVO DESCONHECIDO')) AS "Motivo da Situação Cadastral",
    (e.cod_natureza_juridica || ' - ' || COALESCE(nat.nome_natureza, 'NATUREZA DESCONHECIDA')) AS "Natureza Jurídica",
    (CASE e.cod_porte WHEN '00' THEN 'NÃO INFORMADO' WHEN '01' THEN '01 - MICROEMPRESA' WHEN '03' THEN '03 - PEQUENO PORTE' WHEN '05' THEN '05 - DEMAIS' ELSE '00 - NÃO INFORMADO' END) AS "Porte",
    (e.capital_social) AS "Capital Social",
    (est.cod_cnae_principal || ' - ' || COALESCE(cn.nome_cnae, 'CNAE DESCONHECIDO')) AS "Atividade Principal",
    (COALESCE((
        SELECT GROUP_CONCAT(sc.cod_cnae || ' - ' || COALESCE(cn_sec.nome_cnae, 'CNAE SECUNDÁRIO DESCONHECIDO'), ' / ')
        FROM estabelecimento_cnae_sec sc
        LEFT JOIN cnae cn_sec ON sc.cod_cnae = cn_sec.cod_cnae
        WHERE sc.cnpj_basico = est.cnpj_basico
          AND sc.cnpj_ordem = est.cnpj_ordem
          AND sc.cnpj_dv = est.cnpj_dv
    ), '')) AS "Atividade(s) Secundária(s)",
    (CASE COALESCE(sn.opcao_simples, '') WHEN 'S' THEN 'SIM' ELSE 'NÃO' END) AS "Optante Simples",
    (CASE WHEN sn.data_opcao_simples > '0001-01-01' THEN strftime('%d/%m/%Y', sn.data_opcao_simples) ELSE '' END) AS "Data Opção Simples",
    (CASE WHEN sn.data_exclusao_simples > '0001-01-01' THEN strftime('%d/%m/%Y', sn.data_exclusao_simples) ELSE '' END) AS "Data Exclusão Simples",
    (CASE COALESCE(sn.opcao_mei, '') WHEN 'S' THEN 'SIM' ELSE 'NÃO' END) AS "Optante MEI",
    (CASE WHEN sn.data_opcao_mei > '0001-01-01' THEN strftime('%d/%m/%Y', sn.data_opcao_mei) ELSE '' END) AS "Data Opção MEI",
    (CASE WHEN sn.data_exclusao_mei > '0001-01-01' THEN strftime('%d/%m/%Y', sn.data_exclusao_mei) ELSE '' END) AS "Data Exclusão MEI",
    (TRIM(
        COALESCE(TRIM(est.tipo_logradouro), '') ||
        CASE WHEN TRIM(COALESCE(est.tipo_logradouro, '')) <> '' AND TRIM(COALESCE(est.logradouro, '')) <> '' THEN ' ' ELSE '' END ||
        COALESCE(TRIM(est.logradouro), '') ||
        CASE WHEN TRIM(COALESCE(est.numero, '')) <> '' AND (TRIM(COALESCE(est.tipo_logradouro, '')) <> '' OR TRIM(COALESCE(est.logradouro, '')) <> '') THEN ', ' ELSE '' END ||
        COALESCE(TRIM(est.numero), '') ||
        CASE WHEN TRIM(COALESCE(est.complemento, '')) <> '' AND (TRIM(COALESCE(est.tipo_logradouro, '')) <> '' OR TRIM(COALESCE(est.logradouro, '')) <> '' OR TRIM(COALESCE(est.numero, '')) <> '') THEN ', ' ELSE '' END ||
        COALESCE(TRIM(est.complemento), '')
    )) AS "Endereço",
    (est.bairro) AS "Bairro",
    (COALESCE(mun.nome_municipio, 'MUNICÍPIO DESCONHECIDO')) AS "Município",
    (est.uf) AS "Estado (UF)",
    (CASE WHEN est.uf <> 'EX' THEN 'BRASIL' ELSE UPPER(COALESCE(p.nome_pais, 'PAÍS DESCONHECIDO')) END) AS "País",
    (est.cep) AS "CEP",
    ((CASE WHEN NULLIF(TRIM(est.telefone_1), '') IS NOT NULL THEN COALESCE('(' || NULLIF(TRIM(est.ddd_telefone_1), '') || ') ', '') || TRIM(est.telefone_1) END)) AS "Telefone 1",
    ((CASE WHEN NULLIF(TRIM(est.ddd_telefone_2), '') IS NOT NULL THEN COALESCE('(' || NULLIF(TRIM(est.ddd_telefone_2), '') || ') ', '') || TRIM(est.telefone_2) END)) AS "Telefone 2",
    (est.email) AS "E-mail",
    ((
        SELECT GROUP_CONCAT(UPPER(TRIM(s.nome_socio)), ', ')
        FROM socio s
        WHERE s.cnpj_basico = e.cnpj_basico
    )) AS "Sócio(s)"
FROM estabelecimento est
JOIN empresa e ON est.cnpj_basico = e.cnpj_basico
LEFT JOIN simples sn ON e.cnpj_basico = sn.cnpj_basico
LEFT JOIN municipio mun ON est.cod_municipio = mun.cod_municipio
LEFT JOIN motivo mot ON est.cod_motivo_situacao_cadastral = mot.cod_motivo
LEFT JOIN natureza_juridica nat ON e.cod_natureza_juridica = nat.cod_natureza
LEFT JOIN cnae cn ON est.cod_cnae_principal = cn.cod_cnae
WHERE
    est.uf IN ('PE')
    AND est.cod_situacao_cadastral IN ('02')
    AND est.data_inicio_atividade >= '2022-01-01' AND est.data_inicio_atividade <= '2022-01-31'
ORDER BY e.razao_social ASC
LIMIT 20;
```
