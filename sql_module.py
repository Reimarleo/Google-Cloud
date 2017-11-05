''' Credentials (hidden) '''
host='?.?.?.?'
user = '???'
passwd = '???'
     
import pymysql
import time

def get_list_databases():
    try:
        list_databases = list()
        db = pymysql.connect(host=host, user=user, passwd=passwd)
        cursor = db.cursor()
        sql_query = ("show databases")
        cursor.execute(sql_query)
        for (databases) in cursor:
             list_databases.append(databases[0])
    except Exception as e:
        print(e) 
    finally:
        cursor.close()
        db.close()        
    return list_databases

def get_list_tbls_from_db(db_name):
    try:
        list_tables = list()
        db = pymysql.connect(host=host, user=user, passwd=passwd, db=db_name)
        cursor = db.cursor()
        sql_query = ("show tables")
        cursor.execute(sql_query)
        for (databases) in cursor:
             list_tables.append(databases[0])
    except Exception as e:
        print(e) 
    finally:
        cursor.close()
        db.close()    
    return list_tables

def create_db(db_to_create):
    if db_to_create == '':
        return
    try:
        db = pymysql.connect(host=host, user=user, passwd=passwd)
        cursor = db.cursor()
        sql_query = 'CREATE DATABASE ' + db_to_create + ';'
        cursor.execute(sql_query)
    except Exception as e:
        print(e) 
    finally:
        cursor.close()
        db.close()  
    return

def run_query_on_db(db_name, sql_query):
    try:
        db = pymysql.connect(host=host,
                             user=user,
                             passwd=passwd,
                             autocommit=True,
                             charset='utf8',
                             db=db_name)
        cursor = db.cursor()
        cursor.execute(sql_query)
        time.sleep(2)
    except Exception as e:
        print(e) 
    finally:
        cursor.close()
        db.close()     
    return

def is_table_empty(db_name, tbl_name):
    try:
        bool_table_empty = True
        db = pymysql.connect(host=host, user=user, passwd=passwd, db=db_name)
        cursor = db.cursor()
        sql_query = 'SELECT COUNT(*) from ' + tbl_name + ';'
        cursor.execute(sql_query)
        rowcount = cursor.fetchone()[0]
        if rowcount > 0:
            bool_table_empty = False
    except Exception as e:
        print(e) 
    finally:
        cursor.close()
        db.close()  
    return bool_table_empty





