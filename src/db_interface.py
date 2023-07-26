import vertica_python

DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = '5433'
DEFAULT_USER = 'dbadmin'
DEFAULT_PWD = 'no_default_enter_pwd'
class DBInterface:
    def __init__(self, config):
        # Private attribute to store connection info
        self._conn_info = {
            'host': config('host', DEFAULT_HOST),
            'port': config('port', DEFAULT_PORT),
            # CHANGED FOR A SPECIFIC USECASE
            'database': config.get('vertica-database', DEFAULT_DATABASE),
            'user': config.get('user', DEFAULT_USER),
            'password': config('password', DEFAULT_PWD)
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

    def copy_csv_to_table(self, csv_file_path, table_name, schema):
        # Use the COPY command to copy data from the CSV file into the Vertica table 
        # This is applicable when the file has no value requiring conversions
        with open(csv_file_path, 'rb') as csv_file:
            with self as cursor:
                cursor.copy(f"COPY {schema}.{table_name} FROM STDIN DELIMITER ','", csv_file, buffer_size=65536)

    def get_all_tables(self, schema):
        # Retrieve a list of all table names in the schema
        with self as cursor:
            cursor.execute(f"SELECT table_name FROM v_catalog.tables WHERE table_schema = '{schema}'")
            return [row[0] for row in cursor.fetchall()]

    def get_table_columns(self, table_name, schema):
        # Retrieve a list of all column names in the specified table and schema
        with self as cursor:
            cursor.execute(f"SELECT column_name FROM v_catalog.columns WHERE table_schema = '{schema}' AND table_name = '{table_name}'")
            return [row[0] for row in cursor.fetchall()]

    def copy_table_data_to_csv(self, dest_db_interface, table_name, dest_file_path, schema):
        # Use the COPY command to copy data from the source table to the CSV file
        with dest_db_interface as cursor:
            copy_command = f"COPY {schema}.{table_name} TO STDOUT DELIMITER ','"
            with open(dest_file_path, 'w') as dest_file:
                cursor.copy(copy_command, dest_file)
