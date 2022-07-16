import pandas as pd
import logging


''' daily '''

def mapping_transaction_rate(df, eventTime) -> 'dataframe':
    df['event_date'] = eventTime
    if (df['sum_of_success'].iloc[0] + df['sum_of_fail'].iloc[0]) == 0:
        df['success_rate'] = 0
    else:
        df['success_rate'] = df['sum_of_success'] / (df['sum_of_success'] + df['sum_of_fail'])
    return df

def mapping_transaction_error(df, eventTime) -> 'dataframe':
    df['event_date'] = eventTime
    df['error_rate'] = df['sum_of_error'] / sum(df['sum_of_error'])
    return df



''' weekly '''


