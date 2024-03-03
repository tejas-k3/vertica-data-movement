import json
import os
import argparse
import subprocess
import sys

import paramiko
import vertica_python
import pandas as pd
from db_interface import DBInterface
from data_migration_engine import DataEngine
from libs.db_config import DBConfig


class DataTransfer:
    def __init__(self, src_config, dest_config, data_source, mode, schema, config_reset=False):
        DB.Config(reset=config_reset)
        self.src_config, self.dest_config = get_db_config()
        self.data_source = data_source
        self.mode = mode

    def run(self):
        if self.mode == 'copy':
            self.copy_data()
        elif self.mode == 'import':
            self.import_data()
        elif self.mode == 'export':
            self.export_data()
        else:
            print("Invalid mode. Supported modes are 'copy', 'import', and 'export'.")

    def copy_data(self):
        with DBInterface(self.src_config) as src_db_interface, DBInterface(self.dest_config) as dest_db_interface:
            # Fetch all table names from the source database
            src_tables = self.get_all_tables(src_db_interface)
            # Copy data from source to destination for each table
            for table_name in src_tables:
                schema_match = DataEngine.schema_match(table_name, src_db_interface, dest_db_interface)
                if not schema_match:
                    print(f"Schema mismatch for table '{table_name}' between source and destination databases.")
                    continue
                self.copy_table_data(table_name, src_db_interface, dest_db_interface)

    def get_all_tables(self, db_interface):
        # Retrieve a list of all table names in the specified schema
        return db_interface.get_all_tables(self.schema)

    def copy_table_data(self, table_name, src_db_interface=None, dest_db_interface=None, csv_file=None):
        # THERE HAS TO BE SOMETHING BETTER
        # Note :- Gather all schema & tables in the src db, generate
        # a json file let user edit the file to confirm which
        # tables' data to be transferred & accordingly proceed!
        pass
        # # Get the CSV file path for the table data
        # csv_file_path = os.path.join(self.data_source, f"{table_name}.csv")
        # # Copy data from the source table to the CSV file
        # dest_file_path = os.path.join(
        #     self.data_source, f"{table_name}_copy.csv")
        # src_db_interface.copy_table_data_to_csv(
        #     dest_db_interface, table_name, dest_file_path, self.schema)
        # # Copy data from the CSV file to the destination table
        # dest_db_interface.copy_csv_to_table(
        #     dest_file_path, table_name, self.schema)

    def import_data(self):
        # Copy data from all CSV files to their respective tables in the destination database
        with DBInterface(self.dest_config) as dest_db_interface:
            if not os.path.isdir(self.data_source):
                # It's a single file
                data_file = os.path.basename(self.data_source)
                table_name = os.path.splitext(data_file)[0]
                schema_match = DataEngine.schema_match(table_name, dest_db_interface, csv_file=data_file)
                if not schema_match:
                    print(f"Schema mismatch for table '{table_name}' between csv file and database.")
                    return
                self.copy_table_data(table_name, dest_db_interface=dest_db_interface)
                return
            # Get a list of all CSV files in the data source folder
            csv_files = [file for file in os.listdir(
                self.data_source) if file.lowercase().endswith('.csv')]
            for csv_file in csv_files:
                # Extract the table name from the CSV file name
                table_name = os.path.splitext(csv_file)[0]
                DataEngine.schema_match(self, table_name, dest_db_interface=db_interface, csv_file=data_file)
                if not schema_match:
                    print(f"Schema mismatch for table '{table_name}' between csv file and database.")
                    continue
                self.copy_table_data(table_name, dest_db_interface=dest_db_interface)

    def export_data(self):
        # Export data from all tables in the source database to separate CSV files
        # Check if the schema of the table matches between database and CSV

        if not schema_match:
            print(
                f"Schema mismatch for table '{table_name}' between CSV and database.")
            return
        with DBInterface(self.src_config) as src_db_interface:
            # Fetch all table names from the source database
            src_tables = self.get_all_tables(src_db_interface)
            # Export data from each table to a separate CSV file
            for table_name in src_tables:
                # Get the CSV file path for the table data
                csv_file_path = os.path.join(
                    self.data_source, f"{table_name}.csv")
                # Export data from the table to the CSV file
                src_db_interface.copy_table_data_to_csv(
                    src_db_interface, table_name, csv_file_path, self.schema)

    def converted_copy_csv_to_vertica(self, csv_file_path, table_name, column_conversions=None):
        # Use pandas to read the CSV file into a DataFrame
        df = pd.read_csv(csv_file_path)
        # Convert specified columns using the provided conversion functions
        if column_conversions:
            for col_indices, conversion_func in column_conversions:
                for col_index in col_indices:
                    df.iloc[:, col_index] = df.iloc[:,
                                            col_index].apply(conversion_func)
        # Convert the DataFrame to CSV data (in-memory)
        csv_data = df.to_csv(header=False, index=False, sep=',')
        # Use the COPY command to copy data from the in-memory CSV data into the Vertica table
        with DBInterface(self.dest_config) as db_interface:
            copy_command = f"COPY {self.schema}.{table_name} FROM STDIN DELIMITER ','"
            db_interface.copy_csv_to_table(csv_data, table_name, self.schema)


"""
Get Config
"""
import os

CONFIG_FILE = 'config.json'
SERVICE_CONFIG_FILE = 'raf_service_configuration.json'


class DataMigration:

    def get_config_file(self):
        """
        Get config file
        """
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), CONFIG_FILE)

    def get_service_config(self):
        """
        Get service config file
        """
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config_files\\', SERVICE_CONFIG_FILE)

    def process_query_result(self, result):
        """
        Get service config file
        """
        result_list = []
        for output in result:
            result_list.append(output[0])

        return result_list

    def execute_shell(self, cmd):
        """
        Helper to execute shell commands
        :param cmd:
        :return:
        """
        proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        results = proc.communicate()
        if proc.returncode != 0 and len(results[1]) > 0:
            raise IOError("Error: {} {}".format(cmd, results))
        return results


class ConfdCommandInterface:

    def __init__(self, host, username='admin', password='password'):
        self._host = host
        self._username = username
        self._password = password

    def ssh_client_connection(self):
        client = paramiko.SSHClient()
        # Automatically add the virtual machine's host key
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        # Connect to the virtual machine
        client.connect(self._host, username=self._username, password=self._password, port=42002)
        return client

    def ssh_command_execution(self, command):
        # connecting to ssh client
        client = self.ssh_client_connection()

        stdin, stdout, stderr = client.exec_command(command)
        # Read the command output
        output = stdout.read().decode().strip()
        return stdout, stderr

    def ssh_commands_execution(self, commands):
        # connecting to ssh client
        client = self.ssh_client_connection()
        stdin, stdout, stderr = client.exec_command("\n".join(commands))
        output = stdout.read().decode().strip()
        return output, stderr


if __name__ == "__main__":
    # parser = argparse.ArgumentParser(description="Data Transfer CLI Tool")
    # parser.add_argument("-src-db", "--source-database",
    #                     help="Source database configuration file")
    # parser.add_argument("-dest-db", "--destination-database",
    #                     help="Destination database configuration file")
    # parser.add_argument("-data-src", "--data-source",
    #                     help="Data source (folder containing CSVs or a single CSV)")
    # parser.add_argument(
    #     "-mode", help="Mode of operation: 'copy', 'import', or 'export'")
    # args = parser.parse_args()
    # src_config = sys.argv[1]
    # dest_config = sys.argv[2]
    DM = DataMigration()
    config_file = DM.get_config_file()
    with open(config_file) as conf_file:
        config = json.load(conf_file)
    source_db = DBInterface(config, 'source')
    # data_transfer_app = DataTransferApp(
    #     src_config, dest_config, args.data_source, args.mode)
    # data_transfer_app.run()
    create_schema = 'CREATE TABLE IF NOT EXISTS schema_columns(schema_name VARCHAR(128),table_name VARCHAR(128),' \
                    'column_name VARCHAR(128),data_type VARCHAR(128)) '
    source_db.execute(create_schema, [])
    populate_schema = 'INSERT INTO schema_columns SELECT DISTINCT table_schema,table_name,column_name,data_type FROM ' \
                      'v_catalog.columns'
    source_db.execute(populate_schema, [])
    get_schema = 'select distinct(schema_name) from schema_columns'
    schemas = DM.process_query_result(source_db.execute(get_schema, []))
    for schema in schemas:
        get_tables = "select distinct(table_name) from schema_columns where schema_name='%s'" % schema
        tables = DM.process_query_result(source_db.execute(get_tables, []))
        for table in tables:
            get_varbinary_columns = "select column_name from schema_columns where table_name='{}' and data_type ilike " \
                                    "'varbinary%'".format(table)
            get_columns = "select column_name from schema_columns where table_name='{}' and data_type not ilike " \
                          "'varbinary%'".format(table)
            varbinary_columns = DM.process_query_result(source_db.execute(get_varbinary_columns, []))
            columns = DM.process_query_result(source_db.execute(get_columns, []))
            copy_query = "docker exec ids-database /opt/vertica/bin/vsql -U dbadmin -w $(grep admin_password " \
                         "/data/vertica/root/insights-storage.cfg | cut -d ' ' -f 3) -F $';' -At -o /tmp/%s.csv " \
                         "-c 'SELECT " % table
            for column in varbinary_columns:
                copy_query += 'to_hex({}),'.format(column)
            for column in columns:
                copy_query += '{},'.format(column)
            copy_query = copy_query[:-1]
            copy_query += " FROM %s.%s;'" % (schema, table)
            ssh_class = ConfdCommandInterface(config['database']['source']['host'])
            commands = ['unhide netintact', 'kvasthilda', 'netintact', copy_query]
            output, stderr = ssh_class.ssh_commands_execution(commands)
            output = list(filter(None, output.strip().split("\r\n")))
            print("Successfully created backup csvs")
    drop_table = 'DROP TABLE schema_columns'
    schema = source_db.execute(drop_table, [])
