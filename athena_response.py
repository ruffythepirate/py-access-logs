import boto3

_athena = boto3.client('athena')

def get_result_generator(query_id):
    result = _query_next(query_id)
    rows = result['ResultSet']['Rows'].__iter__()
    columns_row = next(rows)
    should_continue = True

    while should_continue:
        for row in rows:
            yield create_result(columns_row, row)
        next_token = result['NextToken']
        if next_token is not None:
            result = _query_next(query_id, next_token)
        should_continue = next_token is not None


def _query_next(query_id, token=None):
    if(token is None):
        return _athena.get_query_results(
            QueryExecutionId = query_id,
            MaxResults = 500)
    else:
        return _athena.get_query_results(
            QueryExecutionId = query_id,
            MaxResults = 500,
            NextToken=token)

def create_result(first_row, current_row):
    column_definitions = _get_values(first_row)
    values = _get_values(current_row)
    result = {}
    for col, val in zip(column_definitions, values):
        result[col] = val
    return result



def _get_values(row):
    properties = row['Data']
    extractedArray = []
    for prop in properties:
        extractedArray.append(prop['VarCharValue'])
    return extractedArray

