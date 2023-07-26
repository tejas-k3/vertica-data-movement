import os
import argparse
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

    def copy_table_data(self, table_name, src_db_interface=None, dest_db_interface=None, csv_file = None):
        # THERE HAS TO BE SOMETHING BETTER
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
                DataEngine.schema_match(self, table_name, dest_db_interface=db_interface, csv_file = data_file)
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
                DataEngine.schema_match(self, table_name, dest_db_interface=db_interface, csv_file = data_file)
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Transfer CLI Tool")
    parser.add_argument("-src-db", "--source-database",
                        help="Source database configuration file")
    parser.add_argument("-dest-db", "--destination-database",
                        help="Destination database configuration file")
    parser.add_argument("-data-src", "--data-source",
                        help="Data source (folder containing CSVs or a single CSV)")
    parser.add_argument(
        "-mode", help="Mode of operation: 'copy', 'import', or 'export'")
    args = parser.parse_args()
    src_config = self.get_config(args.source_database)
    dest_config = self.get_config(args.destination_database)
    data_transfer_app = DataTransferApp(
        src_config, dest_config, args.data_source, args.mode)
    data_transfer_app.run()
