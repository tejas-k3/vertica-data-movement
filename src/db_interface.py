import vertica_python
class DBInterface:
    """
    Database Interface for DB operations
    """
    def __init__(self, config):
        # Private attribute to store connection info
        self._conn_info = {
            'host': config['host'],
            'port': config['port'],
            'database': config['database'],
            'user': config['user'],
            'password': config['password']
        }

    def __enter__(self):
        # Establish a connection to the Vertica database and return the cursor
        self._connection = vertica_python.connect(**self._conn_info)
        self._cursor = self._connection.cursor()
        return self._cursor

    def __exit__(self, exc_type, exc_value, traceback):
        # Close the cursor and connection when the context exits
        self._cursor.close()
        self._connection.close()
