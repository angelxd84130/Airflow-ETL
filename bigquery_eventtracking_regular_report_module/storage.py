import pandas as pd
from datetime import datetime
import pymysql
import logging



def db_connect():
    logging.info('connecting to local db..')
    conn = pymysql.connect(host="127.0.0.1", user="root", password='zxcv1234', port=3306, db='BetSlip')
    cur = conn.cursor()
    
    return cur, conn

def save_transaction_error(df):
    cur, conn = db_connect()
    logging.info('connecting successful. writting transaction_error in db..')
    for rows in df.itertuples():
        rows = list(tuple(rows)[1:])
        rows = [str(i) for i in rows]
        sql = '''INSERT INTO EventTracking.transaction_error_count (event_date, error_code, sum_of_error, error_rate)
            VALUES ("%s", "%s", "%s", "%s");
            ''' % tuple(rows)

        cur.execute(sql)
        conn.commit()

    logging.info('writting successfully!')
    

def save_transaction_success_rate(df):
    cur, conn = db_connect()
    logging.info('connecting successful. writting transaction_success_rate in db..')

    for rows in df.itertuples():
        rows = list(tuple(rows)[1:])
        rows = [str(i) for i in rows]
        sql = '''INSERT INTO EventTracking.transaction_success_rate (event_date, sum_of_success, sum_of_fail, fail_rate)
            VALUES ("%s", "%s", "%s", "%s");
            ''' % tuple(rows)

        cur.execute(sql)
        conn.commit()

    logging.info('writting successfully!')
