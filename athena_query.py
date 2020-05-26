import boto3
athena = boto3.client('athena')


def start_query(query):
    return athena.start_query_execution(
        QueryString=query,
        ClientRequestToken='local-tes1'*4,
        QueryExecutionContext = {
            'Database':'default'
        },
        ResultConfiguration = {
            'OutputLocation': 's3://athena-access-logs-athenaqueryresults-11d2eczrmrqf4/'
        },
        WorkGroup='primary'
    )['QueryExecutionId']

def run_access_log_query():
    queryExecutionId = start_query("select date, request_ip, user_agent, uri, count(*) as num from cloudfront_logs where status = 200 AND uri <> '/css/an-old-hope.css' AND user_agent NOT IN('Sogou%20web%20spider/4.0(+http://www.sogou.com/docs/help/webmasters.htm%2307)') group by date, request_ip, user_agent, uri")
    return queryExecutionId

def main():
    queryExecutionId = run_access_log_query()

    get_query_execution(queryExecutionId)
    result = get_query_result(queryExecutionId)
    print(result['ResultSet']['Rows'][0])
    print(result['ResultSet']['Rows'][1])


def get_query_result(queryExecutionId):
    result = athena.get_query_results(QueryExecutionId = queryExecutionId)
    return result

def get_query_execution(queryId):
    execution = athena.get_query_execution(QueryExecutionId = queryId)
    return execution

def extract_column_definitions(result):
    properties = result['ResultSet']['Rows'][0]['Data']
    extractedArray = []
    for prop in properties:
        extractedArray.append(prop[0]['VarCharValue'])




if __name__ == '__main__':
    main()


