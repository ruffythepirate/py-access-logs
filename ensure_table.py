import boto3

_glue = boto3.client('glue')
_athena = boto3.client('athena')

_create_table_template = """CREATE EXTERNAL TABLE IF NOT EXISTS %s.cloudfront_logs (
  `date` DATE,
  time STRING,
  location STRING,
  bytes BIGINT,
  request_ip STRING,
  method STRING,
  host STRING,
  uri STRING,
  status INT,
  referrer STRING,
  user_agent STRING,
  query_string STRING,
  cookie STRING,
  result_type STRING,
  request_id STRING,
  host_header STRING,
  request_protocol STRING,
  request_bytes BIGINT,
  time_taken FLOAT,
  xforwarded_for STRING,
  ssl_protocol STRING,
  ssl_cipher STRING,
  response_result_type STRING,
  http_version STRING,
  fle_status STRING,
  fle_encrypted_fields INT,
  c_port INT,
  time_to_first_byte FLOAT,
  x_edge_detailed_result_type STRING,
  sc_content_type STRING,
  sc_content_len BIGINT,
  sc_range_start BIGINT,
  sc_range_end BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '%s'
TBLPROPERTIES ( 'skip.header.line.count'='2' )"""

def ensure_table(location, database = 'default'):
    try:
        _glue.get_table(
            DatabaseName=database,
            Name='cloudfront_logs')
        return '%s.cloudfront_logs' % (database)
    except _glue.exceptions.EntityNotFoundException:
        print('Could not find table cloudfront_logs, creating it')
        return create_table(location, database)


def _create_table(location, database):
    from ensure_bucket import ensure_athena_bucket
    import uuid
    athena_bucket = ensure_athena_bucket()
    exec_id = _athena.start_query_execution(
        QueryString=_create_table_template % (database, location),
        ClientRequestToken=uuid.uuid1(),
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': 's3://%s/' % (athena_bucket.name)
        })['QueryExecutionId']
    _wait_for_query(exec_id)
    return '%s.cloudfront_logs' % (database)


def _wait_for_query(query_id, max_wait_time = 10):

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
    ensure_table(location='test')

if __name__ == '__main__':
    main()


