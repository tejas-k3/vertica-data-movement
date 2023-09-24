import vertica_python
from vertica_python import connect, errors  # pylint: disable=import-error

DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = '5433'
DEFAULT_USER = 'dbadmin'
DEFAULT_PWD = 'no_default_enter_pwd'


class DBInterface:
    def __init__(self, config, feature='forensics'):
        """
        @param config: Insights DB Connection parameters
        """
        self.iteration_count = 0
        for data_source_item in config['database']:
            if data_source_item == feature:
                self.conn_info = {'host': config['database'][data_source_item]['host'],
                                  'user': config['database'][data_source_item].get('username', 'dbadmin'),
                                  'password': config['database'][data_source_item].get('password', ''),
                                  'database': config['database'][data_source_item].get('name', 'insights_data'),
                                  'use_prepared_statements': True}

    def connect_to_vertica(self):
        """
        Establishes connection to Vertica DB
        @return: Connection
        """

        self.conn_info['host'] = self.conn_info['host']
        self.conn_info['backup_server_node'] = []

        return connect(**self.conn_info)

    def execute(self, query, params):
        """
        Executes the passed query, query can be prepared statements
        @param query: Query to be executed
        @param params: Query parameters
        """
        with self.connect_to_vertica() as connection:
            cur = connection.cursor()
            cur.execute(query, params, use_prepared_statements=True)
            results = cur.fetchall()
            connection.commit()
            return results

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
            cursor.execute(
                f"SELECT column_name FROM v_catalog.columns WHERE table_schema = '{schema}' AND table_name = '{table_name}'")
            return [row[0] for row in cursor.fetchall()]

    def copy_table_data_to_csv(self, dest_db_interface, table_name, dest_file_path, schema):
        # Use the COPY command to copy data from the source table to the CSV file
        with dest_db_interface as cursor:
            copy_command = f"COPY {schema}.{table_name} TO STDOUT DELIMITER ','"
            with open(dest_file_path, 'w') as dest_file:
                cursor.copy(copy_command, dest_file)
