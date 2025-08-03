# 🛠 Scripts de Automação

Este diretório contém scripts `.bat` para automatizar tarefas comuns do projeto em ambientes **Windows**, como a
instalação de dependências e a execução do processo de ETL.

---

## ⚙️ Instalação Simplificada (Usuários Windows)

Este método é ideal se você quer **apenas usar a ferramenta**, sem precisar de Git ou comandos complexos.

### 📥 Baixe o Projeto

Faça o download da versão mais recente em formato `.zip` a partir da página de **Releases**:
➡️ **[Baixar a Última Versão](https://github.com/msantosjader/rfb-cnpj-etl/releases/latest)**

> Na página, encontre a seção **“Assets”** e clique em **`Source code (zip)`** para baixar.

### 🗂 Extraia os Arquivos

Após o download, **extraia o conteúdo do arquivo `.zip`** para uma pasta de sua preferência.

### 🚀 Execute a Instalação do Ambiente

1. Navegue até a pasta `scripts`.
2. Dê **duplo clique** no arquivo `001_preparar_ambiente.bat`.
3. Uma janela de terminal será aberta e instalará tudo automaticamente.
4. Aguarde a mensagem **"SUCESSO!"**.

### ❗ Não tem Python instalado?

Este projeto requer Python para funcionar. Se o passo anterior falhou, siga estas instruções:

1. **Baixe o Python:**
    * Acesse o site oficial: **[python.org/downloads/windows/](https://www.python.org/downloads/windows/)**
    * Clique no botão para baixar a versão estável mais recente.

2. **Instale o Python:**
    * Execute o instalador que você baixou.
    * **IMPORTANTE:** Na primeira tela da instalação, marque a caixa que diz **`Add python.exe to PATH`** na parte
      inferior da janela. Isso é crucial.
    * Depois de marcar a caixa, clique em `Install Now`.

3. **Execute a Instalação do Ambiente Novamente:**
    * Após a instalação do Python terminar, volte para a pasta `scripts` e execute o `001_preparar_ambiente.bat` mais
      uma vez. Agora ele deverá funcionar.

### ✅ Tudo Pronto!

Agora você pode seguir para a seção abaixo e utilizar os scripts de execução do projeto.

---

## ▶️ Como Executar

Para usar a ferramenta, entre na pasta `scripts` e dê um duplo clique no script de execução desejado.

---

## 📜 Lista de Scripts

### `001_preparar_ambiente.bat`

- **Propósito:** Configura o ambiente do projeto. Cria o ambiente virtual (`.venv`) e instala as dependências do
  `requirements.txt`.
- **Quando usar:** Uma única vez, logo após baixar o projeto e garantir que o Python está instalado.

---

### `002.0_run_complete.bat`

- **Propósito:** Executa o ciclo **completo** do ETL — baixa os dados mais recentes da Receita Federal e os carrega no
  banco de dados.
- **Quando usar:** Para uso geral da ferramenta. **Este é o script principal.**

---

### `002.1_run_download.bat`

- **Propósito:** Executa apenas a **etapa de download** dos dados da Receita Federal.
- **Quando usar:** Se quiser apenas baixar os arquivos, sem carregá-los no banco imediatamente.

---

### `002.2_run_load.bat`

- **Propósito:** Executa apenas a **etapa de carga** dos dados no banco, utilizando arquivos já baixados anteriormente.
- **Quando usar:** Se os dados já foram baixados e você quer (re)carregá-los no banco.

---

## 🐧 Suporte para Linux e macOS

> Arquivos `.sh` com as mesmas funcionalidades poderão ser adicionados futuramente para compatibilidade com ambientes
> Linux/macOS.
