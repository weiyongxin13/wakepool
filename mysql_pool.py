from pymysqlpool import ConnectionPool

config = {
    'pool_name': 'test',
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'root',
    'database': 'sdkmanagedb',
    'charset': 'utf8',
    'enable_auto_resize': True
}


def connection_pool():
    # Return a connection pool instance
    pool = ConnectionPool(**config)
    return pool


if __name__ == '__main__':
    with connection_pool().cursor() as cur:
        try:
            cur.execute("select * from app")
            a = cur.fetchall()
            print("success")
        except Exception as e:
            print("error", e)
    print(a)
