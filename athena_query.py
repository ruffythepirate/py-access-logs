import boto3
_athena = boto3.client('athena')

_access_log_query = """select date, request_ip, user_agent, uri, count(*) as num
                    from cloudfront_logs
                    where status = 200
                        AND uri <> '/css/an-old-hope.css'
                        AND user_agent NOT IN('Sogou%20web%20spider/4.0(+http://www.sogou.com/docs/help/webmasters.htm%2307)')
                    group by date, request_ip, user_agent, uri"""

def run_access_log_query():
    query_id = start_query(_access_log_query)
    _wait_for_query(query_id)
    return query_id

def run_query(query):
    query_id = start_query( query)
    _wait_for_query(query_id)
    return query_id

def start_query( query, database = 'default'):
    import uuid
    from ensure_bucket import get_output_location
    output_location = get_output_location()
    return _athena.start_query_execution(
        QueryString=query,
        ClientRequestToken=uuid.uuid1().hex,
        QueryExecutionContext = {
            'Database':database
        },
        ResultConfiguration = {
            'OutputLocation': output_location
        },
        WorkGroup='primary'
    )['QueryExecutionId']

def _wait_for_query(query_id, max_wait_time = 10):
    from time import sleep
    state = 'RUNNING'
    wait_time = 0
    while state == 'RUNNING' and wait_time < max_wait_time:
        exec_info = _athena.get_query_execution(QueryExecutionId = query_id)['QueryExecution']
        state = exec_info['Status']['State']
        sleep(1)
        wait_time = wait_time + 1

    if state == 'RUNNING':
        raise('Query is still running, but timeout time reached (%s s)' % (max_wait_time))
    return  state



def main():
    print('main')

def get_query_result(queryExecutionId):
    result = _athena.get_query_results(QueryExecutionId = queryExecutionId)
    return result

def get_query_execution(queryId):
    execution = _athena.get_query_execution(QueryExecutionId = queryId)
    return execution

def extract_column_definitions(result):
    properties = result['ResultSet']['Rows'][0]['Data']
    extractedArray = []
    for prop in properties:
        extractedArray.append(prop[0]['VarCharValue'])




if __name__ == '__main__':
    main()


