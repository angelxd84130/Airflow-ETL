from bigquery_eventtracking_regular_report_module import api
from bigquery_eventtracking_regular_report_module import storage
import logging
import time
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator



def get_bigquery_eventtracking():

    # convert time zone from utc+0 to utc+8
    '''
    # for loading current data
    eventTime =  datetime.now().strftime('%Y-%m-%d')
    startTime = (datetime.now() + timedelta(days=-1)).strftime('%Y-%m-%d') + 'T16:00:00'
    endTime = datetime.now().strftime('%Y-%m-%d') + 'T16:00:00'
    
    '''
    # for loading past data
    for n in range(0,2):
        eventTime = (datetime.now() + timedelta(days=-n)).strftime('%Y-%m-%d')
        startTime = (datetime.now() + timedelta(days=-n-1)).strftime('%Y-%m-%d') + 'T16:00:00'
        endTime = (datetime.now() + timedelta(days=-n)).strftime('%Y-%m-%d') + 'T16:00:00'

        logging.info('loading data by date:' + eventTime)

        # download data (Extrat+Transform)
        transaction_rate = api.get_daily_transaction_rate(startTime, endTime, eventTime)
        transaction_error = api.get_daily_transaction_error(startTime, endTime, eventTime)
        sport_transaction_result = api.get_daily_sport_transaction(startTime, endTime, eventTime)
        sport_transaction_error = api.get_daily_sport_error(startTime, endTime, eventTime)


        # storage data (Load)
        storage.save_transaction_rate(transaction_rate, eventTime)
        storage.save_transaction_error(transaction_error, eventTime)
        storage.save_daily_sport_transaction(sport_transaction_result, eventTime)
        storage.save_daily_sport_error(sport_transaction_error, eventTime)
        time.sleep(0.5)


default_args = {
    'owner': 'angel',
    'depends_on_past': False,
    'email': ['angel@glaibo.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'bigquery_eventtracking_regular_report',
        default_args=default_args,
        description='download eventtracking from bigquery server to make a regular report',
        schedule_interval=timedelta(minutes=10),
        start_date=datetime(2022, 7, 1),
        catchup=False,
        tags=['report'],
) as dag:
    p1 = PythonOperator(task_id='bigquery_eventtracking_report',
                        python_callable=get_bigquery_eventtracking)

    p1
