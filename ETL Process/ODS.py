import os
import sys
import csv
#import xlrd
import psycopg2
from config import *
import requests, zipfile
from io import StringIO
import pandas as pd
import datetime
from datetime import *
from pandas import DataFrame
from datetime import datetime
import shutil

DATE_FORMAT = "%Y-%m-%d"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

def init_db():
    params_stg = config_stg()
    conn_stg = psycopg2.connect(**params_stg)
    
    params_ods = config_ods()
    conn_ods = psycopg2.connect(**params_ods)
    
    return conn_stg, conn_ods

def insert_ods_table(conn_ods,df_final,ods_table):
    tmp_df = os.path.dirname(os.path.realpath(__file__)) + '/temp_file/' + ods_table + '.csv'
    df_final.to_csv(tmp_df,index=False,header=True)
    f = open(tmp_df, 'r')
    cursor_ods = conn_ods.cursor()
    try:
        sql_delete = """
        delete from %s  
        
        """% (ods_table)                
#         where extract(month from to_date(cast(date_id as varchar(50)),'YYYYMMDD')) >= extract(month from current_date) - 2        
        
        sql_copy = """
        COPY %s(date_id,contract_id,prev_contract_id,prev_open,prev,mo,last,prev_last,change,high,low,volume,oi,spread,ma_200,ma_50) 
        FROM '%s' DELIMITER ',' CSV HEADER encoding 'utf-8'; 
        """% (ods_table,tmp_df)
        cursor_ods.execute(sql_delete)
        cursor_ods.execute(sql_copy)
        conn_ods.commit()
        os.remove(tmp_df)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn_ods.rollback()
        cursor_ods.close()
        return 1
    print("Insert ODS " + ods_table + " done")
    cursor_ods.close()

def process_ods_table(stg_table, ods_table,conn_stg,conn_ods):
    sql_date = """select * from ods_date"""
    sql_contract = """select * from ods_contract"""
    sql_barchart_newyork_arabica_price = """
    select 
    contract,
    coalesce(lag(contract, '-1'::integer) OVER (PARTITION BY mo ORDER BY snapshot_date)::text,'NaN') AS prev_contract,
    last,
    lag(last, '-1'::integer) OVER (PARTITION BY mo ORDER BY snapshot_date) AS prev_last,
    round(last::numeric - lag(cast(last as float), 1) OVER (PARTITION BY snapshot_date ORDER BY mo::integer DESC)::numeric, 2) AS spread,
    avg(cast(last as float)) OVER (PARTITION BY mo ORDER BY snapshot_date ROWS BETWEEN 200 PRECEDING AND CURRENT ROW) AS ma_200,
    avg(cast(last as float)) OVER (PARTITION BY mo ORDER BY snapshot_date ROWS BETWEEN 50 PRECEDING AND CURRENT ROW) AS ma_50,
    timing,
    mo,
    change,
    prev_open,
    high,
    low,
    prev,
    volume,
    oi,
    snapshot_date
    from %s    
    
    """% (stg_table)    
#     where extract(month from snapshot_date) >= extract(month from current_date) - 2
    
    cursor_ods = conn_ods.cursor()
    cursor_ods.execute(sql_date)
    tuples_date = cursor_ods.fetchall()
    col_dt = [desc[0] for desc in cursor_ods.description]
    cursor_ods.close()
    
    cursor_ods = conn_ods.cursor()
    cursor_ods.execute(sql_contract)
    col_contract = [desc[0] for desc in cursor_ods.description]
    tuples_contract = cursor_ods.fetchall()
    cursor_ods.close()
    
    cursor_stg = conn_stg.cursor()
    cursor_stg.execute(sql_barchart_newyork_arabica_price)
    tuples_stg = cursor_stg.fetchall()
    col_stg = [desc[0] for desc in cursor_stg.description]
    cursor_stg.close()
    
    df_date = pd.DataFrame(tuples_date,columns=col_dt)
    df_contract = pd.DataFrame(tuples_contract,columns=col_contract)
    df_contract['prev_contract_code'] = df_contract['contract_code']
    df_stg = pd.DataFrame(tuples_stg,columns=col_stg)
    df_stg = df_stg.rename(columns={"snapshot_date":"date_actual","contract":"contract_code","prev_contract":"prev_contract_code"})
    
    df_final = pd.merge(pd.merge(pd.merge(df_stg,df_date[['date_actual','date_id']],on="date_actual",how='left'),df_contract[['contract_id','contract_code']],on="contract_code",how="left"),df_contract[['contract_id','prev_contract_code']],on="prev_contract_code", how='left')
    df_final = df_final[['date_id','contract_id_x','contract_id_y','prev_open','prev','mo','last','prev_last','change','high','low','volume','oi','spread','ma_200','ma_50']]    
    df_final.columns = ['date_id','contract_id','prev_contract_id','prev_open','prev','mo','last','prev_last','change','high','low','volume','oi','spread','ma_200','ma_50']
    #print(df_final.dtype())
    #df_final = df_final.fillna('')
    #df_final = df_final.astype({"prev_contract_id": int})
    df_final['prev_contract_id'] = df_final['prev_contract_id'].astype('Int64')
    insert_ods_table(conn_ods,df_final,ods_table)        
    

if __name__ == '__main__':
    # print(os.path.expanduser('~') + '/research/python_import_research_data/research/data_source/cot_ny_report/')    
    conn_stg, conn_ods = init_db()
    ods_table = 'ods_barchart_cotton'
    stg_table = 'stg_barchart_cotton'
    script_name = 'ods_barchart_cotton.py'
    
    start = datetime.now()
    process_ods_table(stg_table,ods_table,conn_stg, conn_ods)
   
         
    end = datetime.now()
    
    duration = end - start
#     print('Duration: ' + duration)

    conn = None
    