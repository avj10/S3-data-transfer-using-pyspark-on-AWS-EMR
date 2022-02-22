from configparser import ConfigParser

parser = ConfigParser()
parser.read('conf/config_data.ini')

'''
Read user input from 'config_data.ini' file.
'''

MYSQL_CONFIG=parser['mySQL_Config']
SOURCE_S3_CONNECTOR =parser['source_s3_connector']
DESTINATION_S3_CONNECTOR =parser['destination_s3_connector']
