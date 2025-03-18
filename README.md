# Fundos-CVM

Este projeto tem como objetivo fazer o download de um arquivo CSV de fundos de investimento da CVM, processar os dados e inseri-los em uma tabela de um banco de dados SQL Server.

## Requisitos

- Python 3.x
- Bibliotecas Python:
  - `requests`
  - `pandas`
  - `pyodbc`

## Instalação

1. Clone o repositório:
   ```bash
   git clone https://github.com/AdonesMelo/Fundos-CVM.git
   cd Fundos-CVM
   ```

2. Instale as dependências:
   ```bash
   pip install requests pandas pyodbc
   ```

## Uso

1. Edite o arquivo `main.py` para configurar as variáveis `server`, `database`, `username` e `password` com as credenciais do seu banco de dados SQL Server.

2. Execute o script:
   ```bash
   python main.py
   ```

## Estrutura do Código

### Funções

- **download_csv(url, nome_arquivo)**: Faz o download de um arquivo CSV a partir de uma URL e o salva localmente.
- **tratar_csv(nome_arquivo)**: Lê o arquivo CSV, remove caracteres indesejados da coluna `CNPJ_FUNDO` e remove duplicatas.
- **conexao_db(server, database, username, password)**: Estabelece conexão com o banco de dados SQL Server.
- **create_table(cursor)**: Cria a tabela `FUNDOS` no banco de dados, removendo-a se já existir.
- **insert_data(cursor, df)**: Insere os dados do DataFrame na tabela `FUNDOS` em lotes.
- **main()**: Função principal que executa o download, processamento, conexão com o banco de dados, criação da tabela e inserção dos dados 
