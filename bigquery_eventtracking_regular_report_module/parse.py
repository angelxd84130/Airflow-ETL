import pandas as pd
import logging

# error_codes

def count_error_codes(StartDate: 'datetime', df: 'dataframe') -> 'dataframe':
    logging.info('counting fail reasons..')
    df = df[(df['actionValue5'] == 'false')]
    total = len(df)
    contents = df[['actionValue6']].drop_duplicates(keep='first')
    result = []
    print(contents)
    for index, row in contents.iterrows():
        content = row['actionValue6']
        data = df[(df['actionValue6'] == content)]
        result.append([StartDate, content, len(data), len(data)/total])
    logging.info('writing to DB:[eventDate, error_code, sum_of_error, error_rate]')
    result = pd.DataFrame(result, columns=['eventDate', 'error_code', 'sum_of_error', 'error_rate'])
    print(result)
    return result



# success_rate

def count_transaction_succes(df: 'dataframe') -> int:
    logging.info('succes counting..')
    df = df[(df['actionValue5'] == 'true')]
    df = df[['actionValue5']]
    return len(df)


def count_transaciton_fail(df: 'dataframe') -> int:
    logging.info('fail counting..')
    df = df[(df['actionValue5'] == 'false')]
    df = df[['actionValue5']]
    return len(df)


def get_daily_counted_data(StartDate: 'datetime', df: 'dataframe'):
    sum_of_success = count_transaction_succes(df)
    logging.info('succes#:' + str(sum_of_success))
    sum_of_fail = count_transaciton_fail(df)
    logging.info('fail#:' + str(sum_of_fail))

    if (sum_of_success + sum_of_fail) == 0:
        logging.info('get data error')
        return [StartDate, sum_of_success, sum_of_fail, 0]
    success_rate = sum_of_success / (sum_of_success + sum_of_fail)
    logging.info('succes_rate:' + str(success_rate))
    result = [[StartDate, sum_of_success, sum_of_fail, success_rate]]
    logging.info('writing to DB:[eventDate, sum_of_success, sum_of_fail, succes_rate]')
    result = pd.DataFrame(result, columns=['eventDate', 'sum_of_success', 'sum_of_fail', 'succes_rate'])
    print(result)
    return result
