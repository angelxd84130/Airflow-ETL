from bigquery_eventtracking_regular_report_module import _query
from bigquery_eventtracking_regular_report_module import parse

''' daily '''

def get_daily_transaction_rate(startTime, endTime, eventTime) -> 'dataframe':
    df = _query.get_daily_transaction_rate(startTime, endTime)
    df = parse.mapping_transaction_rate(df ,eventTime)
    print(df)
    return df

def get_daily_transaction_error(startTime, endTime, eventTime) -> 'dataframe':
    df = _query.get_daily_transaction_error(startTime, endTime)
    df = parse.mapping_transaction_error(df, eventTime)
    print(df)
    return df


def get_daily_sport_transaction(startTime, endTime, eventTime) -> 'dataframe':
    df = _query.get_daily_sport_transaction(startTime, endTime)
    df = parse.mapping_sport_transaction(df, eventTime)
    print(df)
    return df


def get_daily_sport_error(startTime, endTime, eventTime) -> 'dataframe':
    df = _query.get_daily_sport_error(startTime, endTime)
    df = parse.mapping_sport_error(df, eventTime)
    print(df)
    return df
