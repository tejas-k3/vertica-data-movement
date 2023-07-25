import os
import maskpass
import json
from cryptography.fernet import Fernet

CURRENT_DIRECTORY = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(CURRENT_DIRECTORY, 'db_config.json')

class DBConfig:
    def __init__(self, config_file, encryption_key, reset = False):
        self.config_file = config_file
        self.encryption_key = encryption_key
        self.reset = reset
    
    def encrypt(self, data):
        cipher_suite = Fernet(self.encryption_key)
        encrypted_data = cipher_suite.encrypt(data.encode())
        # Convert the encrypted bytes to a Base64-encoded string as
        # byte object can't be stored in JSON, it'll result in error
        return encrypted_data.decode()

    def decrypt(self, encrypted_data):
        cipher_suite = Fernet(self.encryption_key)
        decrypted_data = cipher_suite.decrypt(encrypted_data.encode()).decode()
        return decrypted_data

    def create_config_file(self):
        if self.reset:
            os.remove(self.config_file)
        with open(self.config_file, 'w') as file:
            config_value = {
                "source_database": {
                    "host": input("Enter Vertica Source DB host IP: "),
                    "vertica-database": input("Enter Vertica Source DB Cluster Name: "),
                    "user": input("Enter Source DB username: "),
                    "password": self.encrypt(maskpass.askpass(prompt="Enter Source DB password: ", mask="*"))
                },
                "destination_database": {
                    "host": input("Enter Vertica Destination DB host IP: "),
                    "vertica-database": input("Enter Vertica Destination DB Cluster Name: "),
                    "user": input("Enter Destination DB username: "),
                    "password": self.encrypt(maskpass.askpass(prompt="Enter Destination DB password: ", mask="*"))
                }
            }
            json.dump(config_value, file)

    def get_db_config(self):
        if not os.path.exists(self.config_file) or self.reset:
            print("Damn! Looks like the config file doesn't exist OR reset is requested.")
            print("Let's create a template for you! Fill in the details.")
            self.create_config_file()

        try:
            with open(self.config_file) as file:
                value = json.load(file)
                src_config = value.get("source_database", {})
                dest_config = value.get("destination_database", {})
                # Check if required fields are present in the configurations
                required_fields = ["host", "vertica-database", "user", "password"]
                if not all(field in src_config for field in required_fields):
                    raise ValueError("Source database configuration is missing required fields.")
                if not all(field in dest_config for field in required_fields):
                    raise ValueError("Destination database configuration is missing required fields.")
                # Decrypt the password
                src_config["password"] = self.decrypt(src_config["password"])
                dest_config["password"] = self.decrypt(dest_config["password"])
                return src_config, dest_config
        except Exception as err:
            print(f"Fuk I failed! This is due to {err}")

if __name__ == "__main__":

    # Used Fernet.generate_key() to generate key
    ENCRYPTION_KEY = b'Nbdl8-XaSvp9uZsTM3Hmnixc9nSlmJ9BKf7w6xpyVMA='
    db_config = DBConfig(CONFIG_FILE, ENCRYPTION_KEY)
    src_config, dest_config = db_config.get_db_config()

    # print("Source Config:", src_config)
    # print("Destination Config:", dest_config)

