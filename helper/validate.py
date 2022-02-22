# Import Conf Package
from conf.constants import *
from conf.responses import *
from conf.queries import *

# Import Helper Package
from helper.mysql_connector import MysqlConnector

import boto3, botocore

mysql_connector_object=MysqlConnector()

def validate_source_and_destination_s3_connector():
    try:
        if validate_source_connector():
            print("Source S3 Connector Validation Successful")
        else:
            print(ERROR_MESSAGE + "Source S3 Connector Validation Failed")
            return False

        if validate_destination_connector():
            print("Destination S3 Connector Validation Successful")
        else:
            print(ERROR_MESSAGE + "Destination S3 Connector Validation Failed")
            return False
        if not mysql_connector_object.insert_spark_job_data():
            return False
        return True
    except Exception as e:
        print(EXCEPTION_ERROR_MESSAGE + str(e) + " in validation of source and destination s3 connector")
        return False


def validate_source_connector():
    try:
        session = boto3.session.Session(aws_access_key_id=SOURCE_S3_CONNECTOR["access_key"],
                                        aws_secret_access_key=SOURCE_S3_CONNECTOR["secret_key"])
        s3 = session.resource('s3')
        if not validate_bucket(s3, SOURCE_S3_CONNECTOR["bucket_name"]):
            return False
        if not validate_bucket_file_path(s3, SOURCE_S3_CONNECTOR["bucket_name"], SOURCE_S3_CONNECTOR["file_path"]):
            return False
        return True
    except Exception as e:
        print(EXCEPTION_ERROR_MESSAGE + str(e) + " in validation of source s3 connector")
        return False


def validate_destination_connector():
    try:
        session = boto3.session.Session(aws_access_key_id=DESTINATION_S3_CONNECTOR["access_key"],
                                        aws_secret_access_key=DESTINATION_S3_CONNECTOR["secret_key"])
        s3 = session.resource('s3')
        if not validate_bucket(s3, DESTINATION_S3_CONNECTOR["bucket_name"]):
            return False
        return True
    except Exception as e:
        print(EXCEPTION_ERROR_MESSAGE + str(e) + " in validation of source s3 connector")
        return False


def validate_bucket(s3, bucket_name):
    '''
    Validate s3 bucket name weather exit or no.
    '''
    try:
        s3.meta.client.head_bucket(Bucket=bucket_name)
        return True
    except botocore.exceptions.ClientError as e:
        # If a client error is thrown, then check that it was a 404 error.
        # If it was a 404 error, then the bucket does not exist.
        error_code = int(e.response['Error']['Code'])
        if error_code == 403:
            print(ERROR_MESSAGE + "Private Bucket. Forbidden Access!")
            return False
        elif error_code == 404:
            print(ERROR_MESSAGE + "Bucket Does Not Exist!")
            return False
        print(EXCEPTION_ERROR_MESSAGE + str(e) + " in validating bucket exist")
        return False


def validate_bucket_file_path(s3, bucket_name, file_path):
    '''
    Validate file and folder path of bucket weather exit or not.
    '''
    try:
        my_bucket = s3.Bucket(bucket_name)
        file_prefix = file_path.split("/")[0]
        objects = my_bucket.objects.filter(Prefix=file_prefix)
        my_list = list()
        for obj in objects:
            my_list.append(obj.key)
        if len(my_list) > 0:
            for element in my_list:
                if file_path in element:
                    return True
        print(ERROR_MESSAGE + "No such file path Exist!")
        return False
    except Exception as e:
        print(EXCEPTION_ERROR_MESSAGE + str(e) + " in validating file path exist")
        return False
