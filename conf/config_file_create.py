from configparser import ConfigParser

config = ConfigParser()

config['mySQL_Config'] = {
    'mySQL_hostname': '',
    'mySQL_username': '',
    'mySQL_password': '',
    'mySQL_dbname': '',
}

config['source_s3_connector'] = {
    'access_key': '',
    'secret_key': '',
    'bucket_name': '',
    'file_path': '',
    'file_format': ''
}

config['destination_s3_connector'] = {
    'access_key': '',
    'secret_key': '',
    'bucket_name': '',
    'file_path': '',
    'file_format': ''
}

with open('config_data.ini', 'w') as f:
  config.write(f)

