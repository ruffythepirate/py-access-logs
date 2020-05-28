
def main():
    from athena_query import run_access_log_query
    from athena_response import get_result_generator

    query_id = run_access_log_query()
    generator = get_result_generator(query_id)

    for row in generator:
        print(row)

if __name__ == '__main__':
    main()
