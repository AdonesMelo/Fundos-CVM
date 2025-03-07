import requests
import pandas as pd
import pyodbc
import os

cvs_url = 'https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv'
csv_arquivo = 'cad_fi.csv'

response = requests.get(cvs_url)
print(response)

# Download CSV
if response.status_code == 200:
    with open(csv_arquivo, 'wb') as arquivo:
        arquivo.write(response.content)
        print(f'Download com sucesso! Nome do arquivo: {csv_arquivo}')
else:
    print(f'Erro no Download! Codigo do status code: {response.status_code}')

# Lendo do dataframe
df = pd.read_csv(csv_arquivo, sep=';', encoding='latin1')
print('Leitura do arquivo com sucesso!')
print(df.head())

# Trata coluna "CNPJ_FUNDO" retira "./-"
df['CNPJ_FUNDO'] = df['CNPJ_FUNDO'].str.replace(r'[./-]', '', regex=True)
print('Tratado com sucesso!')
print(df.head())

# Removendo os "CNPJ_FUNDO" duplicados
df = df.drop_duplicates(subset='CNPJ_FUNDO', keep='first')
print('CNPJ_FUNDO duplicados, removido com sucesso!')

# Conectar ao SQL Server
server = ''  # Nome do servidor
database = ''  # Nome do banco de dados
username = ''  # Nome de usuário
password = ''  # Senha

# String de conexão
conexao_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

# Conectando ao banco de dados
conexao = pyodbc.connect(conexao_str)
print('Conectado ao banco de dados com sucesso!')
cursor = conexao.cursor()

# Criando tabela no SQL
criar_tabela_query = '''
IF OBJECT_ID('CREDIREAL', 'U') IS NOT NULL DROP TABLE CREDIREAL;
CREATE TABLE CREDIREAL (
    TP_FUNDO VARCHAR(50),
    CNPJ_FUNDO VARCHAR(14) PRIMARY KEY,
    DENOM_SOCIAL NVARCHAR(MAX),
    DT_REG DATE,
    DT_CONST DATE,
    CD_CVM INT,
    DT_CANCEL DATE,
    SIT VARCHAR(50),
    DT_INI_SIT DATE,
    DT_INI_ATIV DATE,
    DT_INI_EXERC DATE,
    DT_FIM_EXERC DATE,
    CLASSE VARCHAR(50),
    DT_INI_CLASSE DATE,
    RENTAB_FUNDO NVARCHAR(200),
    CONDOM VARCHAR(50),
    FUNDO_COTAS CHAR(1),
    FUNDO_EXCLUSIVO CHAR(1),
    TRIB_LPRAZO CHAR(1),
    PUBLICO_ALVO VARCHAR(50),
    ENTID_INVEST CHAR(1),
    TAXA_PERFM FLOAT,
    INF_TAXA_PERFM NVARCHAR(MAX),
    TAXA_ADM FLOAT,
    INF_TAXA_ADM NVARCHAR(MAX),
    VL_PATRIM_LIQ DECIMAL(18,2),
    DT_PATRIM_LIQ DATE,
    DIRETOR VARCHAR(200),
    CNPJ_ADMIN VARCHAR(18),
    ADMIN VARCHAR(300),
    PF_PJ_GESTOR CHAR(2),
    CPF_CNPJ_GESTOR VARCHAR(20),
    GESTOR VARCHAR(300),
    CNPJ_AUDITOR VARCHAR(20),
    AUDITOR VARCHAR(300),
    CNPJ_CUSTODIANTE VARCHAR(20),
    CUSTODIANTE VARCHAR(300),
    CNPJ_CONTROLADOR VARCHAR(20),
    CONTROLADOR VARCHAR(300),
    INVEST_CEMPR_EXTER CHAR(1),
    CLASSE_ANBIMA VARCHAR(300)
);
'''

# Executa a consulta no SQL e criar a tabela
cursor.execute(criar_tabela_query)
print('Tabela criada com sucesso!')

# Confirma as alteraçãoes no banco de dados
conexao.commit()

# Tratar os valores nulos e garantir que os tipos estão correntos
df = df.fillna({
    'TP_FUNDO': '',
    'CNPJ_FUNDO': '',
    'DENOM_SOCIAL': '',
    'DT_REG': pd.NaT,
    'DT_CONST': pd.NaT,
    'CD_CVM': 0,
    'DT_CANCEL': pd.NaT,
    'SIT': '',
    'DT_INI_SIT': pd.NaT,
    'DT_INI_ATIV': pd.NaT,
    'DT_INI_EXERC': pd.NaT,
    'DT_FIM_EXERC': pd.NaT,
    'CLASSE': '',
    'DT_INI_CLASSE': pd.NaT,
    'RENTAB_FUNDO': '',
    'CONDOM': '',
    'FUNDO_COTAS': '',
    'FUNDO_EXCLUSIVO': '',
    'TRIB_LPRAZO': '',
    'PUBLICO_ALVO': '',
    'ENTID_INVEST': '',
    'TAXA_PERFM': 0.0,
    'INF_TAXA_PERFM': '',
    'TAXA_ADM': 0.0,
    'INF_TAXA_ADM': '',
    'VL_PATRIM_LIQ': 0.0,
    'DT_PATRIM_LIQ': pd.NaT,
    'DIRETOR': '',
    'CNPJ_ADMIN': '',
    'ADMIN': '',
    'PF_PJ_GESTOR': '',
    'CPF_CNPJ_GESTOR': '',
    'GESTOR': '',
    'CNPJ_AUDITOR': '',
    'AUDITOR': '',
    'CNPJ_CUSTODIANTE': '',
    'CUSTODIANTE': '',
    'CNPJ_CONTROLADOR': '',
    'CONTROLADOR': '',
    'INVEST_CEMPR_EXTER': '',
    'CLASSE_ANBIMA': ''
})

# Dividindo em lote para melhora a inclusão
tamanho_lote = 5000
for inicio in range(0, len(df), tamanho_lote):
    fim = inicio + tamanho_lote
    tamanho = df[inicio:fim]

    # Preparndo os dados para inclusão
    dados_incluidos = [
        (
            row['TP_FUNDO'], row['CNPJ_FUNDO'], row['DENOM_SOCIAL'], row['DT_REG'], row['DT_CONST'], row['CD_CVM'], row['DT_CANCEL'], row['SIT'], row['DT_INI_SIT'], row['DT_INI_ATIV'], row['DT_INI_EXERC'], row['DT_FIM_EXERC'], row['CLASSE'], row['DT_INI_CLASSE'], row['RENTAB_FUNDO'], row['CONDOM'], row['FUNDO_COTAS'], row['FUNDO_EXCLUSIVO'], row['TRIB_LPRAZO'], row['PUBLICO_ALVO'], row['ENTID_INVEST'], row['TAXA_PERFM'], row['INF_TAXA_PERFM'], row['TAXA_ADM'], row['INF_TAXA_ADM'], row['VL_PATRIM_LIQ'], row['DT_PATRIM_LIQ'], row['DIRETOR'], row['CNPJ_ADMIN'], row['ADMIN'], row['PF_PJ_GESTOR'], row['CPF_CNPJ_GESTOR'], row['GESTOR'], row['CNPJ_AUDITOR'], row['AUDITOR'], row['CNPJ_CUSTODIANTE'], row['CUSTODIANTE'], row['CNPJ_CONTROLADOR'], row['CONTROLADOR'], row['INVEST_CEMPR_EXTER'], row['CLASSE_ANBIMA']
        )
        for index, row in tamanho.iterrows()
    ]

    # Incluíndo os dados no SQL
    cursor.executemany('''
        INSERT INTO CREDIREAL (TP_FUNDO, CNPJ_FUNDO, DENOM_SOCIAL, DT_REG, DT_CONST, CD_CVM, DT_CANCEL, SIT, DT_INI_SIT, DT_INI_ATIV, DT_INI_EXERC, DT_FIM_EXERC, CLASSE, DT_INI_CLASSE, RENTAB_FUNDO, CONDOM, FUNDO_COTAS, FUNDO_EXCLUSIVO, TRIB_LPRAZO, PUBLICO_ALVO, ENTID_INVEST, TAXA_PERFM, INF_TAXA_PERFM, TAXA_ADM, INF_TAXA_ADM, VL_PATRIM_LIQ, DT_PATRIM_LIQ, DIRETOR, CNPJ_ADMIN, ADMIN, PF_PJ_GESTOR, CPF_CNPJ_GESTOR, GESTOR, CNPJ_AUDITOR, AUDITOR, CNPJ_CUSTODIANTE, CUSTODIANTE, CNPJ_CONTROLADOR, CONTROLADOR, INVEST_CEMPR_EXTER, CLASSE_ANBIMA)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', dados_incluidos)
    conexao.commit()
    print(f'Dados do lote {inicio}-{fim} incluídos com sucessos')
print('Todos os dados incluídos com sucesso!')

# Fechar a conexão
cursor.close()
conexao.close()

# Remover o arquivo CSV 
os.remove(csv_arquivo)