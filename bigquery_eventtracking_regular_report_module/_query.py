import os
import pandas as pd
from google.cloud import bigquery


def _setup_credential():
    credential_path = "/home/albert/airflow/dags/bigquery_eventtracking_regular_report_module/sg-prod-readonly-303206-cb8365379fd6.json"
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path


_setup_credential()
_client = bigquery.Client()



def get_eventTracking_data_by_date(startTime: str = None, endTime: str = None) -> 'dataframe':
    if startTime is None or endTime is None:
        raise ValueError("time parameter must be specified, ")

    query = f"""
        SELECT * FROM `sg-prod-303206.prod.sportsUserBehaviour` 
        WHERE DATE(createTime)>="{startTime}" and DATE(createTime)<"{endTime}"
        and eventId in (200005, 200006, 200007, 200008, 200009)

    """

    query_info = _client.query(query)
    query_info = query_info.to_dataframe()
    return query_info


