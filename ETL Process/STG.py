import os
import sys
import re
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
from selenium.common.exceptions import TimeoutException
import io
# from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from shutil import copyfile, move
import csv
import datetime, calendar, dateparser
from datetime import *
from dateutil.relativedelta import relativedelta
import time
#import xlrd
import tabula
import pandas as pd
from configparser import ConfigParser
import psycopg2
from selenium.webdriver.firefox.options import Options
import glob
import camelot
import requests
from pdfminer.converter import TextConverter
from pdfminer.pdfinterp import PDFPageInterpreter
from pdfminer.pdfinterp import PDFResourceManager
from pdfminer.pdfpage import PDFPage
import urllib.request as urllib2
from googletrans import Translator
import shutil
import PyPDF2
# importing the modules
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import requests
from bs4 import BeautifulSoup
# Import the required Module
import tabula
from tabula.io import read_pdf
import camelot
import pdftables_api
from pdfminer import high_level
from datetime import datetime, timedelta, date
import pytz
from pytz import timezone
from pathlib import Path


def config(filename='config.ini', section='cotton_stg'):
    # create a parser
    parser = ConfigParser()
    parser.read(filename)
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(
            'Section {0} not found in the {1} file'.format(section, filename))
    return db

def get_meta_data(table_name, conn):     
    sql_select = """ 
    select * from public.stg_meta_data where target_name = '%s' 
    """%(table_name)
    df_meta = pd.read_sql(sql_select,conn)
    source_path = df_meta['source_path'].values[0]
    source_name = df_meta['source_name'].values[0]
    temp_name = df_meta['temp_name'].values[0]
    
    return source_name, source_path, temp_name


def insert_into_staging(source_path,temp_name,table_name):
    params = config()
    conn = psycopg2.connect(**params)
    cursor = conn.cursor()
    
    df = pd.read_csv(source_path)
    columns = df.columns
    source_row = (pd.read_csv(source_path,header=None,low_memory=False)).shape[0] 
    # insert data to tmp table
    tmp_df = '/tmp/temp_usda_report.csv'
    df.to_csv(tmp_df, index=False, header=True)
    sql_truncate ="""
    truncate table stg.%s
    """%(temp_name)
    sql_copy = """
        COPY stg.%s(%s) 
        FROM '%s' DELIMITER ',' CSV HEADER encoding 'utf-8'; 
    """% (temp_name,','.join(columns), tmp_df)
    sql_count_row ="""
    select count(1) row_number from stg.%s
    """%(temp_name)
    target_row = (pd.read_sql_query(sql_count_row, conn))['row_number'].values[0]
    
    sql_delete_duplicate = (""" 
        delete from public.%s A
        where exists(
        select 1 from  stg.%s B
        where 1=1
        and A.country = B.country
        and A.report_date = B.report_date
        )
        """% (table_name,temp_name))
    
    sql_insert_stg="""
    insert into public.%s
    select * from stg.%s
    """%(table_name, temp_name)
    
    try:
        cursor.execute(sql_truncate)
        cursor.execute(sql_copy)
        cursor.execute(sql_delete_duplicate)
        cursor.execute(sql_insert_stg)
        conn.commit()
        #cursor.close()

    except Exception as error:
        print(error)         


    
    return source_row, target_row
    

def checking_logs(script_name,source_name,table_name,source_row,target_row,duration,created_by):
    params = config()
    conn = psycopg2.connect(**params)
    cursor = conn.cursor()
    
    tz = timezone('Asia/Ho_chi_Minh')
    loc_dt = tz.localize(datetime.now())
    snapshot_date = loc_dt.strftime("%Y-%m-%d")
    created_date=  loc_dt.strftime("%Y-%m-%d %H:%M:%S")
    
    sql_checking_logs = ("""insert into public.stg_checking_logs (
    script,
    source_name,
    target_name,
    source_row,
    target_row,
    duration,
    snapshot_date,
    created_date,
    created_by) values (
    '%s',
    '%s',
    '%s',
    '%s',
    '%s',
    '%s',
    '%s',
    '%s',
    '%s')"""% (script_name,source_name,table_name,source_row,target_row,duration,snapshot_date, created_date, created_by ))
    
    cursor.execute(sql_checking_logs)
    conn.commit() 
    


def convert_columns(argument):
    argument1 = argument.lower()
    switcher={
        'beginning' : 'beginning_stocks',
        'country': 'country',
        'production':'production',
        'productio':'production',
        'imports':'imports',
        'total':'total_supply',
        'use':'domestic_use',
        'loss':'loss',
        'exports':'exports',
        'ending': 'ending_stocks',
        'area':'area',
        'domestic':'domestic_use',
        'total dom.': 'total_dom_cons',
        'total.1': 'total_distribute'
    }
    return switcher.get(argument1,argument)



def convert_dataframe(df):
    columns_1=[]
    for g in df.columns:
        columns_1.append(convert_columns(g))
    df.columns = columns_1
    #print(df)
    for i in range(len(df.columns)):
        if df[df.columns[i]].isnull().all() == True:
            if df.columns[i].lower() == 'country':
                # TH ko detect đúng cột country
                df[df.columns[i]] = df[df.columns[i-1]]
            else:
                df[df.columns[i]] = df[df.columns[i+1]]
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
                      
    df = df.replace(',', '', regex=True)
    df = df.dropna(axis='columns', how ='all').iloc[1:, :]
    df = df.dropna(thresh=2)
    df_1 = pd.DataFrame()
    index1 =0 
    for i in df.columns:
        if len(i.split(' '))==1:
            df_1[i] = df[i]
        elif len(i.split(' '))!=1:
            if i.lower().find('country') != -1:
                # phát sinh cột country detect chung với cột kế bên nhưng ko lấy giá trị cột kế bên
                df_1[i.split(' ')[0]] = df[i]
                df_1[i.split(' ')[1]] = df[df.columns[index1+1]]
            else:
                #print(pd.DataFrame(df[i].str.split(' ').tolist(), index=df.index))
                df_1[i.split(' ')] = pd.DataFrame(df[i].str.split(' ').tolist(), index=df.index)  
        index1+=1
            
    columns =[]
    for g in df_1.columns:
        columns.append(convert_columns(g)) 
                       
    df_1.columns = columns

    return df_1



def find_page_table(pdf_file, text_find):
    pdffileobj=open(pdf_file,'rb')
    text='Table 05 Cotton Supply and Distribution MY'
    pdfreader=PyPDF2.PdfFileReader(pdffileobj)
    x=pdfreader.numPages
    index=[]
    season =''
    for i in range(x):
        pageobj=pdfreader.getPage(i)
        a= (pageobj.extractText())
        if a.find(text_find) != -1:
            season = a.split(text_find+' ')[1].split(' ')[0][:7]
            #print(a)
            index.append(i+1) 
            
    if len(index)==1:
        index.append(index[0]+1)
        index.append(index[0]+2)
            
    response={'index':index, 'season':season}
    return response

def check_report_date(month_year):
    total_pages= int(page.text)
    reqs = requests.get(url)
    soup = BeautifulSoup(reqs.text, 'html.parser')
    
    
    for link in soup.find_all('a',{'class':'btn btn-info download_btn file_download'}):
        if str(link.get('data-release-date'))[:7] == month_year:
            url = str(link.get('data-release-date'))[:10]
        
    return url


def read_data_report(dest_path,pdf_file, text_find):
    tz = timezone('Asia/Ho_chi_Minh')
    loc_dt = tz.localize(datetime.now())
    snapshot_date = loc_dt.strftime("%Y%m%d")
    created_date=  loc_dt.strftime("%Y-%m-%d %H:%M:%S")
    month_year = loc_dt.strftime("%Y-%m")
    
    processed_path = dest_path +'processed'
    Path(processed_path).mkdir(parents=True, exist_ok=True)
    csv_path = processed_path + '/processed-report-' + snapshot_date + '.csv'
    
    pdffileobj=open(pdf_file,'rb')
    pdfreader=PyPDF2.PdfFileReader(pdffileobj)
    x=pdfreader.numPages
    index=[]
    season =''
    for i in range(x):
        pageobj=pdfreader.getPage(i)
        a= (pageobj.extractText())
        if a.find(text_find) != -1:
            season = a.split(text_find+' ')[1].split(' ')[0][:7]
            index.append(i+1) 
    if len(index)==1:
        index.append(index[0]+1)
        index.append(index[0]+2)

    PDF = tabula.read_pdf(pdf_file, pages=index, multiple_tables=True)
    #print('tới đây rồi')
    list_df=[]
    for i in PDF:
        df = pd.DataFrame(i)
        if df.shape[1] > 5:
            df.to_csv('test.csv',index=False)
            df = convert_dataframe(df)
            list_df.append(df)
    
    df_all = pd.concat(list_df,ignore_index=True)
    columns = df_all.columns
    print(columns)
    df_all[columns[1:]] = df_all[columns[1:]].astype(int)
    report_date = check_report_date(month_year)
    df_all['season']= season
    df_all['snapshot_date']= report_date
    df_all['report_date']= report_date
    df_all['created_date']= created_date
    df_all.to_csv(csv_path, index=False)
        
    return csv_path

            
if __name__ == '__main__':
    
    params = config()
    conn = psycopg2.connect(**params)
    cursor = conn.cursor()
    
    
    tz = timezone('Asia/Ho_chi_Minh')
    loc_dt = tz.localize(datetime.now())
    month_year = loc_dt.strftime("%Y-%m")
    
    url='https://ghoapi.azureedge.net/api/DIMENSION/COUNTRY/DimensionValues'
    reqs = requests.get(url)
    df = pd.DataFrame()
    
    code =[]
    title=[]
    dimension=[]
    parent_dimension =[]
    parent_code =[]
    parent_title=[]
    
    for i in reqs.json()['value']:
        code.append(i['Code'])
        dimension.append(i['Dimension'])
        parent_dimension.append(i['ParentDimension'])
        parent_code.append(i['ParentCode'])
        parent_title.append(i['ParentTitle'])
        
    df['code'] = code
    df['dimension']= dimension
    df['parent_dimension']= parent_dimension
    df['parent_code']= parent_code
    df['parent_title']= parent_title
        
    print(df)
    
    cursor.close()