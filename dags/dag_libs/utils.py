from urllib.parse import urlparse, parse_qs


def uri_to_db(uri) -> dict:
    schema = urlparse(uri)
    params = parse_qs(schema.query)
    result = {
        'db_name': schema.path[1:],
        'db_url': f'{schema.scheme}://{schema.hostname}:{schema.port}',
        'username': schema.username,
        'password': schema.password,
        'verify_ssl_cert': True if params.get('verify_ssl_cert') is None else eval(params.get('verify_ssl_cert')[0]),
        'autocreate': True if params.get('autocreate') is None else eval(params.get('autocreate')[0]),
        'timeout': 3600 if params.get('timeout') is None else params.get('timeout')[0],
        'log_statements': False if params.get('log_statements') is None else eval(params.get('log_statements')[0]),
        }
    return result
