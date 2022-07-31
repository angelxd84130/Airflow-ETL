import os
import pandas as pd
from google.cloud import bigquery


def _setup_credential():
    credential_path = "/home/albert/airflow/dags/bigquery_eventtracking_regular_report_module/sg-prod-readonly-303206-cb8365379fd6.json"
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

_setup_credential()
_client = bigquery.Client()



''' daily '''

def get_daily_transaction_rate(startTime: str=None, endTime: str=None) -> 'dataframe':
    if startTime is None:
        raise ValueError("time parameter must be specified. ")

    query = """
        SELECT sum(if(actionValue5='true', 1, 0)) as sum_of_success, 
                sum(if(actionValue5='false', 1, 0)) as sum_of_fail
                 FROM `sg-prod-303206.prod.sportsUserBehaviour` 
        WHERE createTime between "%s" and "%s"
        and eventId in (200005, 200006, 200007, 200008, 200009, 200080, 200081, 200082)

    """ % (startTime, endTime)

    query_info = _client.query(query)
    query_info = query_info.to_dataframe()
    return query_info


def get_daily_transaction_error(startTime: str=None, endTime: str=None) -> 'dataframe':
    if startTime is None:
        raise ValueError("time parameter must be specified. ")

    query = """
        SELECT actionValue6 as error_code, count(actionValue6) as sum_of_error 
        FROM `sg-prod-303206.prod.sportsUserBehaviour`  
        WHERE createTime between "%s" and "%s" 
        and eventId in (200005, 200006, 200007, 200008, 200009, 200080, 200081, 200082) and actionValue5='false' 
        group by error_code 

    """ % (startTime, endTime)

    query_info = _client.query(query)
    query_info = query_info.to_dataframe()
    return query_info


def get_daily_sport_transaction(startTime: str=None, endTime: str=None) -> 'dataframe':
    if startTime is None:
        raise ValueError("time parameter must be specified. ")

    query = """
        SELECT actionValue1 as sport_code, 
                sum(if(actionValue5='true', 1, 0)) as sum_of_success, 
                sum(if(actionValue5='false', 1, 0)) as sum_of_fail
                 FROM `sg-prod-303206.prod.sportsUserBehaviour` 
        WHERE createTime between "%s" and "%s"
        and eventId in (200005, 200006, 200007, 200008, 200009, 200080, 200081, 200082)
        group by sport_code
        order by sport_code

    """ % (startTime, endTime)

    query_info = _client.query(query)
    query_info = query_info.to_dataframe()
    return query_info



def get_daily_sport_error(startTime: str=None, endTime: str=None) -> 'dataframe':
    if startTime is None:
        raise ValueError("time parameter must be specified. ")

    query = """
        SELECT actionValue1 as sport_code, 
                actionValue6 as error_code, 
                count(actionValue6) as sum_of_error 
                 FROM `sg-prod-303206.prod.sportsUserBehaviour` 
        WHERE createTime between "%s" and "%s" 
        and eventId in (200005, 200006, 200007, 200008, 200009, 200080, 200081, 200082) and actionValue5='false' 
        group by sport_code, error_code 
        order by sport_code, error_code

    """ % (startTime, endTime)

    query_info = _client.query(query)
    query_info = query_info.to_dataframe()
    return query_info



''' weekly '''
