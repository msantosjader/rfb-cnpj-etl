## Ajustes nos dados das tabelas

Foram realizados ajustes (adições e exclusões) para corrigir inconsistências nos dados.
As divergências foram encontradas ao tentar criar chaves estrangeiras em tabelas relacionadas.

---
## 🟢 Adições

### `qualificacao_socio`
Código presente na tabela `empresa`, mas sem correspondência em `qualificacao_socio`.

| cod_qualificacao | nome_qualificacao |
|------------------|-------------------|
| 36               | Gerente-Delegado  |

---

### `motivo` (situação cadastral)
Códigos de motivo de situação cadastral utilizados em `estabelecimento`, mas ausentes na tabela `motivo`.

| cod_motivo | nome_motivo                                                                  |
|------------|-------------------------------------------------------------------------------|
| 32         | DECURSO DE PRAZO DE INTERRUPCAO TEMPORARIA                                   |
| 81         | SOLICITACAO DA ADMINISTRACAO TRIBUTARIA MUNICIPAL/ESTADUAL - SC              |
| 93         | CNPJ - TITULAR BAIXADO                                                       |

---

### `pais`
Códigos de país encontrados em `estabelecimento` e `socio`, mas que não existiam na tabela `pais`.

| cod_pais | nome_pais                               |
|----------|-----------------------------------------|
| 008      | ABU DHABI                               |
| 009      | DIRCE                                   |
| 015      | ALAND, ILHAS                            |
| 150      | JERSEY                                  |
| 151      | CANARIAS, ILHAS                         |
| 200      | CURACAO                                 |
| 321      | GUERNSEY                                |
| 359      | MAN, ILHA DE                            |
| 367      | INGLATERRA                              |
| 393      | JERSEY                                  |
| 449      | MACEDONIA (ANTIGA REP. IUGOSLAVA)       |
| 452      | MADEIRA, ILHA DA                        |
| 498      | MOLDAVIA                                |
| 678      | SAO TOME E PRINCIPE                     |
| 699      | SAO MARTINHO, ILHA DE (PARTE HOLANDESA) |
| 737      | SERVIA                                  |
| 994      | AZERBAIJAO                              |

---

## 🔴 Exclusões

### `simples`
Registros da tabela `simples` que não possuem correspondência nas tabelas `empresa` nem `estabelecimento`.

| cnpj_basico |
|-------------|
| 24417449    |
| 24539162    |
| 30721933    |
| 30728066    |
| 30760363    |
| 30847991    |
| 30857441    |
| 30886793    |
| 30972017    |
