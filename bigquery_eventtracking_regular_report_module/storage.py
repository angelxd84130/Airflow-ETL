import pandas as pd
from datetime import datetime
import pymysql
import logging



def db_connect():
    logging.info('connecting to local db..')
    conn = pymysql.connect(host="127.0.0.1", user="root", password='', port=)
    cur = conn.cursor()
    logging.info('connecting successful.')
    return cur, conn



def save_transaction_error(df, eventTime):
    cur, conn = db_connect()
    table = 'EventTracking.transaction_error_count'

    logging.info('processing transaction_error in db..')
    for index, row in df.iterrows():
        error_code = str(row['error_code'])
        sum_of_error = row['sum_of_error']
        error_rate = row['error_rate']

        logging.info('searching exist in db..' + error_code)
        sql = f"""SELECT * From {table} WHERE event_date="{eventTime}" and error_code="{error_code}"; """
        cur.execute(sql)

        if(cur.fetchone()):
            logging.info('updating data..')
            sql = f""" 
                    UPDATE {table} SET sum_of_error = {sum_of_error}, error_rate = {error_rate} 
                    WHERE event_date = "{eventTime}" and error_code = "{error_code}";
                  """ 
        else:
            logging.info('inserting data..')
            sql = f"""
                    INSERT INTO {table} (event_date, error_code, sum_of_error, error_rate)
                    VALUES ("{eventTime}", "{error_code}", {sum_of_error}, {error_rate});
                """ 
        cur.execute(sql)
        conn.commit()
    logging.info('successfully!')
    


def save_transaction_rate(df, eventTime):
    cur, conn = db_connect()
    table = 'EventTracking.transaction_success_rate'

    logging.info('processing transaction_rate in db..')
    for index, row in df.iterrows():
        sum_of_success = row['sum_of_success']
        sum_of_fail = row['sum_of_fail']
        success_rate = row['success_rate']

        logging.info('searching exist in db..' + eventTime)
        sql = f"""SELECT * From {table} WHERE event_date="{eventTime}"; """
        cur.execute(sql) 

        if(cur.fetchone()):
            logging.info('updating data..')
            sql = f"""
                    UPDATE {table} SET sum_of_success={sum_of_success}, sum_of_fail={sum_of_fail}, success_rate={success_rate}            
                    WHERE event_date = "{eventTime}";
                """
        else:
            logging.info('inserting data..')
        
            sql = f"""
                    INSERT INTO {table} (event_date, sum_of_success, sum_of_fail, success_rate)
                    VALUES ("{eventTime}", {sum_of_success}, {sum_of_fail}, {success_rate});
                """ 
        cur.execute(sql)
        conn.commit()
        
    logging.info('successfully!')
