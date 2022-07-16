from bigquery_eventtracking_regular_report_module import _query
from bigquery_eventtracking_regular_report_module import parse

''' daily '''

def get_daily_transaction_rate(startTime, endTime, eventTime):
    df = _query.get_daily_transaction_rate(startTime, endTime)
    df = parse.mapping_transaction_rate(df ,eventTime)
    print(df)
    return df

def get_daily_transaction_error(startTime, endTime, eventTime):
    df = _query.get_daily_transaction_error(startTime, endTime)
    df = parse.mapping_transaction_error(df, eventTime)
    print(df)
    return df


