import os
import pyodbc, struct
from azure.identity import DefaultAzureCredential, TokenCachePersistenceOptions, InteractiveBrowserCredential, VisualStudioCodeCredential,ManagedIdentityCredential, UsernamePasswordCredential
import pandas as pd
import sqlalchemy as sa
import urllib

server = 'tcp:buse1-apc-d-930864-sqlsv-01.database.windows.net'
database = 'buse1-apc-d-930864-sqldb-01'
driver = 'ODBC Driver 18 for SQL Server'

server = 'tcp:apc-api-server.database.windows.net'
database = 'db-apc-api'

password = ''
try:
    usr = 'rodrigo.v.barbosa'
    original_path = os.getcwd()
    os.chdir(f'C:/Users/{usr}/OneDrive - Unilever/Projetos')
    f = open('banana_file.txt','r')
    password = f.read()
    os.chdir(original_path)
except Exception as e:
    print("Error: ", str(e))
    password = os.environ['password_ul']

def get_conn():        
    login = 'rodrigo.v.barbosa@unilever.com'    
    connection_string = f"Driver={{{driver}}};Server={server};DATABASE={database};UID={login};PWD={password};Authentication=ActiveDirectoryPassword;Encrypt=yes;"    
    connection_url = sa.URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})    
    engine = sa.create_engine(connection_url)
    conn = engine.connect()
    #return conn
    return engine, conn

def get_module():
    engine = get_conn()
    #with engine.begin() as conn:
    conn = engine.connect()
    query = sa.text("SELECT * FROM module")
    df = pd.read_sql_query(query, conn)
    print(df)

#get_module()