from bigquery_eventtracking_regular_report_module import api
from bigquery_eventtracking_regular_report_module import parse
from bigquery_eventtracking_regular_report_module import storage
import logging

from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def transaction_result(StartDate, df):
    df = parse.get_daily_counted_data(StartDate, df)
    return df


def transaction_error(StartDate, df):
    df = parse.count_error_codes(StartDate, df)
    return df


def get_bigquery_eventtracking():

    endTime = datetime.now().strftime('%Y-%m-%d')
    startTime = (datetime.now() + timedelta(days=-1)).strftime('%Y-%m-%d')
    #endTime = (datetime.now() + timedelta(days=-4)).strftime('%Y-%m-%d')
    #startTime = (datetime.now() + timedelta(days=-5)).strftime('%Y-%m-%d')


    logging.info('loading data by date:' + startTime)

    # download data
    df = api.get_daily_event_data(startTime, endTime)

    # get result
    trans_result = transaction_result(startTime, df)
    error_codes = transaction_error(startTime, df)

    # storage data
    storage.save_transaction_success_rate(trans_result)
    storage.save_transaction_error(error_codes)


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
        schedule_interval='@daily',
        start_date=datetime(2021, 11, 7),
        catchup=False,
        tags=['report'],
) as dag:
    p1 = PythonOperator(task_id='bigquery_eventtracking_report',
                        python_callable=get_bigquery_eventtracking)

    p1
