import csv

from libs.column_config import *
import socket


# Note :- IF HEADER ISN'T THERE SHOULD WE FETCH FROM DB & DO?
class DataEngine:

    @staticmethod
    def get_csv_columns(csv_file_path):
        # Read the first row of the CSV file to get column names or count columns
        with open(csv_file_path, 'r') as csv_file:
            reader = csv.reader(csv_file)
            try:
                columns = next(reader)
            except StopIteration:
                # If the header row is missing, count the number of columns in the first row
                print(
                    "Header is missing in this particular file, returning column count instead!")
                # Reset the file pointer to the beginning of the file
                csv_file.seek(0)
                first_row = next(reader)
                column_count = len(first_row)
                return column_count
        return columns

    @staticmethod
    def schema_match(table_name, src_db_interface=None, dest_db_interface=None, csv_file=None):
        # Check if the schema of the table matches between source and destination databases
        # OR between CSV file of the table & table schema in the database.
        src_columns = []
        dest_columns = []
        # It's db to db copy case
        if src_db_interface is not None and dest_db_interface is not None:
            return src_columns == dest_columns
        # One is db, other is CSV
        db_columns = src_db_interface.get_table_columns(table_name, self.src_schema) \
            if src_db_interface is not None else \
            dest_db_interface.get_table_columns(table_name, self.dest_schema)
        csv_columns = get_csv_columns(csv_file)
        # if we get int value (count of columns), header is missing in CSV.
        return len(db_columns) == csv_columns if isinstance(csv_columns, int) \
            else db_columns == csv_columns

    @staticmethod
    def column_conversion_index(db_columns):
        if isInstance(db_columns, int):
            # Can't process, we got int value (count of columns)
            # headers are missing in the CSV.
            return []
        timestampz_column_indexs = set()
        ipv4_column_indexs = set()
        ipv6_column_indexs = set()
        numerical_column_indexs = set()
        # A SPECIAL CASE COULD BE NULL YET TO CONFIRM
        # empty_column_indexs = set()
        for i in range(len(db_columns)):
            if db_columns[i] in TIME_BASED:
                timestampz_column_indexs.add(i)
            elif db_columns[i] in IPV4_BASED:
                ipv4_column_indexs.add(i)
            elif db_columns[i] in IPV6_BASED:
                ipv6_column_indexs.add(i)
            elif db_columns[i] in NUMERICAL_BASED:
                numerical_column_indexs.add(i)
        column_indexes = {
            'timestampz_column_indexs': timestampz_column_indexs,
            'ipv4_column_indexs': ipv4_column_indexs,
            'ipv6_column_indexs': ipv6_column_indexs,
            'numerical_column_indexs': numerical_column_indexs
        }
        return column_indexes

    @staticmethod
    def convert_string_to_varbinary(value):
        # Convert the IP address to bytes in varbinary format
        return bytes(value, encoding='utf-8')

    @staticmethod
    def convert_string_to_timestampz(value, timezone_str='UTC'):
        # Convert the value to a timestamp obj with UTC time zone!
        # UTC is very much important as src, db might be configured
        # to different timezones and this will sync the records.
        dt = datetime.strptime(value, '%Y-%m-%d %H:%M:%S:%f')
        try:
            timezone = pytz.timezone(timezone_str)
        except pytz.UnknownTimeZoneError:
            print(f"Invalid time zone: {timezone_str}")
            return None
        # Assign the time zone to the datetime object
        dt_with_tz = timezone.localize(dt)
        # Convert the datetime object to a timestampz string
        timestampz_str = dt_with_tz.strftime('%Y-%m-%d %H:%M:%S.%f %Z')
        return timestampz_str

    @staticmethod
    def convert_string_to_numeric(value):
        # Convert the value to a numeric type
        # For example, if numeric values use commas as thousands separators:
        # return float(value.replace(',', ''))
        return value

    @staticmethod
    def convert_string_to_int(value):
        # Convert the value to an integer type
        # If integer values are in string format:
        # return int(value)
        return

    @staticmethod
    def convert_string_to_ipv6_varbinary(ipv6_address):
        try:
            binary_address = socket.inet_pton(socket.AF_INET6, ipv6_address)
            return binary_address
        except socket.error:
            raise ValueError(f"Invalid IPv6 address: {ipv6_address}")
