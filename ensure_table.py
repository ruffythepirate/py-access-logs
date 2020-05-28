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
    from athena_query import run_query
    run_query(_create_table_template % (database, location))
    return '%s.cloudfront_logs' % (database)


def main():
    ensure_table(location='test')

if __name__ == '__main__':
    main()


