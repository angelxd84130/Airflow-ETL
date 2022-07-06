from bigquery_eventtracking_regular_report_module import _query


def get_daily_event_data(StartTime: str=None, EndTime: str=None) -> 'dataframe':
    return _query.get_eventTracking_data_by_date(StartTime, EndTime)


