import os
import sys
import pandas as pd

file_dir = os.path.dirname(__file__)
sys.path.append(file_dir)
#sys.path.append(r'C:\Users\Mario\Google Drive\MySQL')

import bq_module
import sql_module

'''
Schema info for public-data:noaa_gsod
country	STRING	NULLABLE
state	STRING	NULLABLE
year	STRING	NULLABLE
temp	FLOAT	NULLABLE	Mean temperature for the day in degrees Fahrenheit to tenths. Missing = 9999.9
'''

''' This function writes the SQL query to produce the desired table '''
def get_qry_for_states():
    sql_query = ''
    sql_query+= 'SELECT country, state, MIN(ROUND((temp-32)*5/9,1)) min_celsius, MAX(ROUND((temp-32)*5/9,1)) max_celsius '
    sql_query+= 'FROM ('
    sql_query+= '      SELECT stn, year, temp, state, country '
    sql_query+= '      FROM ('
    sql_query+= '            SELECT stn, year, temp '
    sql_query+= '            FROM TABLE_QUERY([bigquery-public-data:noaa_gsod], \'table_id CONTAINS "gsod"\') '
    sql_query+= '            WHERE (year LIKE \'199%\' OR year = \'2000\') AND temp != 9999.9 '
    sql_query+= '           ) a '
    sql_query+= '      JOIN [bigquery-public-data:noaa_gsod.stations] b '
    #sql_query+= '      ON a.stn=b.usaf AND a.wban=b.wban '
    sql_query+= '      ON a.stn=b.usaf '
    sql_query+= '      WHERE state IS NOT NULL AND state != \'\' AND country = \'US\' '
    sql_query+= '     ) '
    sql_query+= 'GROUP BY country, state '
    sql_query+= 'ORDER BY state ASC '
    #sql_query+= 'LIMIT 100 '
    sql_query+= ';'
    return sql_query

''' This function builds a Pandas dataframe with the result '''
def build_dataframe(data):
    df = pd.DataFrame(columns=('country', 'state', 'min_celsius', 'max_celsius'))
    i=0
    for row in data:
        i+=1  
        df.loc[i] = [row[0],row[1],row[2],row[3]]
    return df

''' This function writes the SQL query to create the empty destination table in MySQL '''
def get_qry_to_create_tbl():
    sql_query = ''
    sql_query+=('CREATE TABLE final ( ')
    sql_query+=('country VARCHAR(2) NOT NULL, ')
    sql_query+=('state VARCHAR(2) NOT NULL, ')
    sql_query+=('min_celsius FLOAT, ')
    sql_query+=('max_celsius FLOAT, ')
    sql_query+=('PRIMARY KEY (country, state) ) ')    
    return sql_query

''' This function reads the dataframe and writes the SQL query to
load the rows in the destination table '''
def build_qry_from_df(df, destination_tbl):
    sql_query_full = ''
    for index, row in df.iterrows():
        values_to_insert = '('
        values_to_insert+= '\'' + row['country'] + '\','
        values_to_insert+= '\'' + row['state'] + '\','
        values_to_insert+= str(row['min_celsius']) + ','
        values_to_insert+= str(row['max_celsius'])
        values_to_insert+= '); '        
        sql_query = ''
        sql_query+= 'INSERT INTO ' + destination_tbl + ' (country, state, min_celsius, max_celsius) '
        sql_query+= 'VALUES ' + values_to_insert + '\n'        
        sql_query_full += sql_query    
    return sql_query_full
    


if __name__ == '__main__':
    print('Process started')
    
    ''' Run function that writes the SQL query '''
    sql_query = get_qry_for_states()
    
    ''' Get data from BigQuery '''
    data = bq_module.get_table_from_sql_qry(sql_query)
    
    ''' Load data into a Pandas data frame'''
    df = build_dataframe(data)
    
    ''' Get list of databases - create temp_mf if doesn't exist '''    
    list_databases = sql_module.get_list_databases()    
    if 'temp_mf' not in list_databases:
        sql_module.create_db('temp_mf')
        print('Database created')
    else:
        print('Database already exists')
    
    ''' Get list of tables - create empty destination table if doesn't exist '''
    list_tables = sql_module.get_list_tbls_from_db('temp_mf')
    if 'final' not in list_tables:
        sql_query = get_qry_to_create_tbl()
        sql_module.run_query_on_db('temp_mf', sql_query)
        print('Table created')
    else:
        print('Table already exists')    
    
    ''' Check destination table is empty - if so, build query and load rows'''
    destination_tbl_is_empty = sql_module.is_table_empty('temp_mf', 'final')
    if destination_tbl_is_empty:
        sql_query_full = build_qry_from_df(df, 'final')
        sql_module.run_query_on_db('temp_mf', sql_query_full)
        print('Rows loaded')
    else:
        print('Table is not empty')
        
    print('Process ended')
    
    





