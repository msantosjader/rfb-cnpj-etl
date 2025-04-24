# Logs de Execução do ETL da Base de Dados CNPJ (Receita Federal)

## Download da base de dados
```bash
🕒 15:01:31 |⏱️ 00:00:00 |🚀 INICIANDO DOWNLOAD...
🕒 15:01:35 |⏱️ 00:00:04 |🌐 37 ARQUIVOS DISPONÍVEIS NO SITE DA RECEITA FEDERAL (04/2025)
🕑 15:01:37 |⏱️ 00:00:06 |ℹ️ 1 ARQUIVOS RESTANTES. BAIXANDO:
Estabelecimentos9.zip |   0.4%                 | 00:26:49 |  1.25MB de   319MB |   207kB/s
🕒 15:02:54 |⏱️ 00:00:06 |🏁 DOWNLOAD CONCLUÍDO
```

## Carga do banco de dados
```bash
🕒 11:38:03 |⏱️ 00:00:00 |🚀️ INICIANDO TAREFAS DO BANCO DE DADOS
🕒 11:38:05 |⏱️ 00:00:01 |📋 VALIDAÇÃO DOS ARQUIVOS ZIP...
🕒 11:38:05 |⏱️ 00:00:01 |ℹ️ PERÍODO: 04/2025
🕒 11:38:05 |⏱️ 00:00:01 |ℹ️ LOCAL: E:\Python\rfb-cnpj-etl\data\downloads\2025-04
🕒 11:38:05 |⏱️ 00:00:01 |📁 37 ARQUIVOS NA PASTA LOCAL
🕒 11:38:09 |⏱️ 00:00:05 |✅ ARQUIVOS VALIDADOS
🕒 11:38:09 |⏱️ 00:00:05 |📋 CALCULANDO TOTAL DE REGISTROS...
🕒 11:39:22 |⏱️ 00:01:18 |ℹ️ TOTAL DE REGISTROS: 196.894.508
🕒 11:39:22 |⏱️ 00:01:19 |📋 CRIANDO TABELAS...
🕒 11:39:25 |⏱️ 00:01:21 |✅ TABELAS CRIADAS
🕒 11:39:25 |⏱️ 00:01:21 |📋 REALIZANDO CARGA NO BANCO DE DADOS SQLITE...
🕒 11:39:25 |⏱️ 00:01:21 |    LOG DE CARGA NO BANCO DE DADOS, QUE PODE SER:

1. BARRA DE PROGRESSO (DEBUG_LOG = False)
💾 INSERINDO DADOS...   3.05% ███▌                                      [00:49]
OU
2. CADA BATCH INSERIDO NO BANCO DE DADOS, ACUMULANDO AS LINHAS INSERIDAS (DEBUG_LOG = True)
🕒 11:49:28 |⏱️ 00:01:21 |🐞 REGISTROS:       1.359 (  0.00%) | CNAES.ZIP               | FILA:  0 / 22
🕒 11:49:31 |⏱️ 00:01:24 |🐞 REGISTROS:     201.359 (  0.10%) | EMPRESAS0.ZIP           | FILA:  4 / 22
🕒 11:49:34 |⏱️ 00:01:28 |🐞 REGISTROS:     401.359 (  0.20%) | EMPRESAS0.ZIP           | FILA:  8 / 22
🕒 13:00:29 |⏱️ 01:12:23 |🐞 REGISTROS: 196.675.349 ( 99.89%) | SOCIOS9.ZIP             | FILA:  3 / 22
🕒 13:00:31 |⏱️ 01:12:25 |🐞 REGISTROS: 196.875.349 ( 99.99%) | SOCIOS9.ZIP             | FILA:  2 / 22
🕒 13:00:32 |⏱️ 01:12:25 |🐞 REGISTROS: 196.894.499 (100.00%) | SOCIOS9.ZIP             | FILA:  1 / 22

🕒 13:00:32 |⏱️ 01:12:26 |✅ CARGA DE DADOS CONCLUÍDA
🕒 13:00:32 |⏱️ 01:12:26 |📋 APLICANDO CORREÇÕES NA BASE DE DADOS...
🕒 13:05:14 |⏱️ 01:17:07 |✅ CORREÇÕES APLICADAS
🕒 13:05:14 |⏱️ 01:17:07 |📋 CRIANDO ÍNDICES...
🕒 13:06:30 |⏱️ 01:18:24 |ℹ️ [01/18] ÍNDICE CRIADO: idx_empresa_cnpj
🕒 13:09:23 |⏱️ 01:21:16 |ℹ️ [02/18] ÍNDICE CRIADO: idx_empresa_razao_social
🕒 13:10:42 |⏱️ 01:22:36 |ℹ️ [03/18] ÍNDICE CRIADO: idx_empresa_natureza
🕒 13:11:34 |⏱️ 01:23:28 |ℹ️ [04/18] ÍNDICE CRIADO: idx_empresa_qualificacao
🕒 13:12:17 |⏱️ 01:24:10 |ℹ️ [05/18] ÍNDICE CRIADO: idx_empresa_porte
🕒 13:16:14 |⏱️ 01:28:08 |ℹ️ [06/18] ÍNDICE CRIADO: idx_estab_empresa
🕒 13:19:59 |⏱️ 01:31:53 |ℹ️ [07/18] ÍNDICE CRIADO: idx_estab_nome_fantasia
🕒 13:24:25 |⏱️ 01:36:19 |ℹ️ [08/18] ÍNDICE CRIADO: idx_estab_cnae_principal
🕒 13:29:32 |⏱️ 01:41:26 |ℹ️ [09/18] ÍNDICE CRIADO: idx_estab_municipio
🕒 13:33:26 |⏱️ 01:45:20 |ℹ️ [10/18] ÍNDICE CRIADO: idx_estab_data_inicio
🕒 13:38:15 |⏱️ 01:50:08 |ℹ️ [11/18] ÍNDICE CRIADO: idx_estab_data_situacao
🕒 13:43:07 |⏱️ 01:55:01 |ℹ️ [12/18] ÍNDICE CRIADO: idx_estab_uf_municipio
🕒 13:46:07 |⏱️ 01:58:00 |ℹ️ [13/18] ÍNDICE CRIADO: idx_estab_situacao
🕒 13:46:51 |⏱️ 01:58:45 |ℹ️ [14/18] ÍNDICE CRIADO: idx_simples_empresa
🕒 13:47:40 |⏱️ 01:59:34 |ℹ️ [15/18] ÍNDICE CRIADO: idx_socio_empresa
🕒 13:48:02 |⏱️ 01:59:56 |ℹ️ [16/18] ÍNDICE CRIADO: idx_socio_qualificacao
🕒 13:49:09 |⏱️ 02:01:03 |ℹ️ [17/18] ÍNDICE CRIADO: idx_socio_cpf_cnpj
🕒 13:50:23 |⏱️ 02:02:16 |ℹ️ [18/18] ÍNDICE CRIADO: idx_socio_nome
🕒 13:50:23 |⏱️ 02:02:16 |✅ ÍNDICES CRIADOS
🕒 13:50:23 |⏱️ 02:02:17 |📋 ATIVANDO CHAVES ESTRANGEIRAS...
🕒 13:50:23 |⏱️ 02:02:17 |✅ CHAVES ESTRANGEIRAS ATIVADAS
🕒 13:50:23 |⏱️ 02:02:17 |🏁 EXECUÇÃO FINALIZADA | SQLITE | 04/2025
```