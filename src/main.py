import pandas as pd
import sqlalchemy as sa
from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

queryLastMinutes = 60

syncTimeSeconds = {}
collectTimeSeconds = {}
scheduler = None
import os
print("oi")
print(os.getcwd())
import app.database as database
import app.ADXQuery as adx_query
import app.ADXDriver as adx_driver

adx_driver = adx_driver.ADXDriver()

engine,conn = database.get_conn()

Base = declarative_base()
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()


class Module(Base):
    __tablename__ = 'module'    
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String)
    description = sa.Column(sa.String)
    created = sa.Column(sa.String)
    device_id = sa.Column(sa.Integer)
    opcua = sa.Column(sa.String)
    object = sa.Column(sa.String)
    isEnabled = sa.Column(sa.Integer)

class Datalog(Base):
    __tablename__ = 'datalog'    
    id = sa.Column(sa.Integer, primary_key=True)
    ts = sa.Column(sa.String)
    factory_id = sa.Column(sa.Integer)
    module_id = sa.Column(sa.Integer)
    data = sa.Column(sa.String)
    created = sa.Column(sa.String)
 

def runLogs():

    #modules = session.query(Module).all()
    
    now_1 = datetime.now(timezone.utc)    
    now_str2 = now_1.strftime("%Y-%m-%d %H:%M:%S")
    timeToRunInv = (int(queryLastMinutes))  * - 1
    now = now_1 + timedelta(minutes= timeToRunInv)
    timeToRunInv = timeToRunInv * 1
    ago = now + timedelta(minutes= timeToRunInv)
    totalTime = (now - ago)
    totalTime = totalTime.total_seconds()/60        
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    ago_str = ago.strftime("%Y-%m-%d %H:%M:%S")
                
    print(' xxx Schedule running at ' + now_str2 + ' >  from ' + ago_str + ' to ' + now_str + '  xxx ')

    query_modules = sa.text("SELECT * FROM module")
    modules = pd.read_sql_query(query_modules, conn)

    query_devices = sa.text("SELECT * FROM device")
    devices = pd.read_sql_query(query_devices, conn)

    query_factories = sa.text("SELECT * FROM factory")
    factories = pd.read_sql_query(query_factories, conn)
    
    for i in range(len(modules)):
        try:
            #organiza toda a estrutura de dados
            module = modules.iloc[i]        
            device = devices.loc[devices['id'] == module.id]
            device = device.iloc[0]
            factory = factories.loc[factories['id'] == device.factory_id]
            factory = factory.iloc[0]
            query_module_tags = sa.text(f"SELECT * FROM tag where module_id = {module['id']}")        
            module_tags = pd.read_sql_query(query_module_tags, conn)

            print(f"Running logs to module {module.object} from factory {factory['name']}")

            tags = module_tags['address']
            #monta tags em lista, conforme a query precisa
            all_tags = str(list(tags))
            #monta query
            query = adx_query.createQuery3(ago_str, now_str, all_tags, factory['name'], module.object)
            #cria um dataframe a partir da query criada
            dataFromAdx = adx_driver.executeQuery(query)
            
            #pega a coluna data, que está em dict e transforma em dataframe            
            d1 = dataFromAdx['data'].map(lambda x: dict(eval(x)))
            d2 = d1.apply(pd.Series)
            #deleta coluna data
            dataFromAdx.drop('data', axis=1, inplace=True)
            #busca endereços e nomes padronizados das tags
            translate_to_macs = dict(module_tags[['address', 'name']].values)
            #renomeia para as colunas ficarem no padrao
            tagValues = d2.rename(columns=translate_to_macs)
            #transforma o dataframe traduzido em uma lista de dicionarios para a coluna novamente
            dataFromAdx['data'] = tagValues.to_dict('records')

            columns = ['ts', 'factory_id', 'module_id', 'data', 'created']

            # Construir a lista de colunas e placeholders
            columns_str = ', '.join(columns)
            placeholders = ', '.join([f":{col}" for col in columns])
            table_name = 'datalog'
            # Preparar a consulta SQL
            sql = sa.text(f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})")
            # Preparar os dados para inserção
            data_to_insert = []
            for index, row in dataFromAdx.iterrows():
                row_data = {
                    'ts': row['ts'],
                    'factory_id': int(factory['id']),
                    'module_id': int(module['id']),
                    'data': str(row['data']),
                    'created': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                }
                data_to_insert.append(row_data)

            # Executar a inserção em massa usando executemany
            conn.execute(sql, data_to_insert)
            # Confirmar a transação
            conn.commit()
            # Encerrando conexão
            print(f"All records inserted in {table_name} successfully.")

            #metrix calculation
            #[TODO]
            #
        except Exception as e:            
            end = datetime.now(timezone.utc)
            print("Error: ", str(e))                    

    end = datetime.now(timezone.utc)
    time_to_process = (end- now_1).total_seconds()
    print("Processing takes " + str(time_to_process) + " seconds")
    print('xxxxxxxxxxxxxxxxxxxxxxxx  Schedule END  xxxxxxxxxxxxxxxxxxxxxxxx')
    conn.close()
    
runLogs()