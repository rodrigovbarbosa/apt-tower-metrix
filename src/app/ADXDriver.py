from datetime import datetime
from datetime import timedelta
import os

from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.helpers import dataframe_from_result_table

usr = 'rodrigo.v.barbosa'
username = f"{usr}@unilever.com"
password = None
import os

try:
    usr = usr
    original_path = os.getcwd()
    os.chdir(f'C:/Users/{usr}/OneDrive - Unilever/Projetos')
    f = open('banana_file.txt','r')
    password = f.read()
    os.chdir(original_path)
except Exception as e:
    print("Error: ", str(e))
    password = os.environ['password_ul']

        
tenant_id = 'f66fae02-5d36-495b-bfe0-78a6ff9f8e6e'
cluster_id = 'https://dfazuredataexplorer.westeurope.kusto.windows.net'

class ADXDriver():

    def __init__(self):
        pass
    
    def executeQuery(self, query):
        kcsb = KustoConnectionStringBuilder.with_aad_user_password_authentication(cluster_id, username, password, tenant_id)
        kc = KustoClient(kcsb)
        db = "dfdataviewer"
        
        inicioConsulta = datetime.now()
        try:
            response = kc.execute(db, query)    
            fimConsulta = datetime.now()
            kusto_temp = dataframe_from_result_table(response.primary_results[0])
            print(" >>> >>> Dados entre ", kusto_temp["ts"].iloc[-1], " e ", kusto_temp["ts"].iloc[0], " - Query Time: " + str((fimConsulta - inicioConsulta).total_seconds()), ' - ', len(kusto_temp), ' Dados.')            
        except Exception as e:
            print(e)
            return []
        return kusto_temp
    
    def createQuery(self, start, end, tags):
        query2 = "".join([   
        "let arrayTags = dynamic(",
        "", tags,"",
        ");",
        "let timezone = 0;",    
        "let start = datetime(", start, ");",
        #"let second_start = datetime_part('Second',start); ",
        #"let offset_start_time_second = datetime_add('second', -second_start, start); ",
        "let start_time = datetime_add('hour', -timezone, start); ",    
        "let end = datetime(", end, ");",
        #"let second_end = datetime_part('Second', end); ",
        #"let offset_end_time_second = datetime_add('second', -second_end, end); ",
        "let end_time = datetime_add('hour', -timezone, end); ",
        "let datTab = materialize( CommonAmerica",
        "| project TS, Tag, SiteId, Value ",
        "| where Tag has_any (arrayTags)",
        "| where TS between (start_time .. end_time));",
        "let datTab2 = datTab",
        "| project TS, Tag, SiteId, Value = iff(isnotnull(todouble(Value)), todouble(Value), iff(Value == 'True', todouble(1), iff(Value == 'OMO', todouble(2), iff(Value == 'BRILHANTE', todouble(3), iff(Value == 'SURF', todouble(4), todouble(0))))));"
        "let filledTable = datTab2",
        #"let filledTable = datTab",
        "| make-series max(todouble(Value)) default=int(null) on TS from start_time to end_time step 30s by Tag, SiteId",
        "| project TS, Tag, fill_data=series_fill_forward(max_Value), SiteId",
        "| project TS, Tag, fill_data_all=series_fill_backward(fill_data), SiteId",
        "| mv-expand TS, fill_data_all",
        "| project TS = format_datetime(todatetime(TS),'yyyy-MM-dd HH:mm:ss'), Value=todouble(fill_data_all), Tag, SiteId;",
        "let filteredTable = filledTable",
        "| project TS = format_datetime(datetime_add('hour', 0, todatetime(TS)),'yyyy-MM-dd HH:mm:ss'), Tag, Value",
        "| evaluate pivot(Tag, any(Value)) ",
        "| order by TS asc | as timeseries;",
        "filteredTable"])
        return query2

    def createConnectionAndQuery(self, start_str, end_str, tags):
        kcsb = KustoConnectionStringBuilder.with_aad_user_password_authentication(cluster_id, username, password, tenant_id)
     
        kc = KustoClient(kcsb)
        db = "dfdataviewer"

        strArray = str(list(tags))
        inicioConsulta = datetime.now()
        try:
            response = kc.execute(db, self.createQuery(start_str, end_str, strArray))
            fimConsulta = datetime.now()
            kusto_temp = dataframe_from_result_table(response.primary_results[0])
            print(" >>> >>> Dados entre ", kusto_temp["TS"].iloc[-1], " e ", kusto_temp["TS"].iloc[0], " - Query Time: " + str((fimConsulta - inicioConsulta).total_seconds()), ' - ', len(kusto_temp), ' Dados.')            
        except Exception as e:
            print(e)
            return []
        return kusto_temp

    def get(self, tags, driver):

        simulateHour = str(os.getenv('simulateHour'))
        if simulateHour == 'True':
            #get start hour from somewhere
            now = driver.startingHour
            #add time to run to next run            
            t1 = timedelta(seconds=int(timeToStepInSeconds))
            driver.startingHour = driver.startingHour + t1
        else:
            now = datetime.now()

        agoReader = int(os.getenv('agoReader'))
        t1 = timedelta(seconds=agoReader)
        ago2 =  now - t1
        dt_string3 = now.strftime("%Y-%m-%d %H:%M:%S")
        dt_string4 = ago2.strftime("%Y-%m-%d %H:%M:%S")   

        database3 = self.createConnectionAndQuery(dt_string4, dt_string3, tags)
        #print(f" FIm: {dt_string3} - Ultimo dado: {database3.iloc[-1]['TS']}")
        return database3
    
    
    def write(self, tags):
        pass