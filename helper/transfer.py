# Import Conf Package
from conf.constants import *
from conf.responses import *

# Import Helper Package
from helper.mysql_connector import MysqlConnector
from helper.spark_config import *

import boto3,time

mysql_connector_object=MysqlConnector()

def transfer_data():
    '''
    transfer user specified format file from source S3 to destination S3.
    '''
    try:
        start = time.time()
        current_job_id=mysql_connector_object.get_current_job_id()
        if "." in SOURCE_S3_CONNECTOR["file_path"]:
            # For transferring specific file
            data_size = get_data_size()
            spark=get_spark_session(data_size)
            sc = spark.sparkContext
            spark_conf_properties=get_spark_config(sc)
            if not mysql_connector_object.insert_spark_job_log(current_job_id, spark_conf_properties, data_size):
                return False
            if transfer_data_helper(sc, spark, ""):
                print("Data transfer Successful")
                mysql_connector_object.update_job_log_status_and_time(start)
                return True
        else:
            # For transferring specific folder
            file_list, data_size = get_file_list_and_size()
            spark = get_spark_session(data_size)
            sc = spark.sparkContext
            if not mysql_connector_object.insert_spark_job_log(current_job_id, data_size):
                return False
            for file in file_list:
                if not transfer_data_helper(sc, spark, file):
                    return False
            mysql_connector_object.update_job_log_status_and_time(start)
            print("Data transfer Successful")
            return True
        return False
    except Exception as e:
        error_msg = EXCEPTION_ERROR_MESSAGE + str(e) + " in transferring data"
        print(error_msg)
        mysql_connector_object.update_job_log_on_error(error_msg)
        return False


def transfer_data_helper(sc, spark, file):
    try:
        read_df = read_data(sc, spark, file)
        if read_df is not None:
            if write_data(sc, read_df, file):
                return True
        return False
    except Exception as e:
        error_msg = EXCEPTION_ERROR_MESSAGE + str(e) + " in transferring data helper"
        print(error_msg)
        mysql_connector_object.update_job_log_on_error(error_msg)
        return False


def read_data(sc, spark, file):
    try:
        sc._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", SOURCE_S3_CONNECTOR["access_key"])
        sc._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", SOURCE_S3_CONNECTOR["secret_key"])
        read_file_path = "s3a://" + SOURCE_S3_CONNECTOR[
            "bucket_name"] + "/" + SOURCE_S3_CONNECTOR['file_path'] + file
        file_format = SOURCE_S3_CONNECTOR["file_format"]
        if file_format == "csv":
            df = spark.read.format("csv").option("header", "true").load(read_file_path)
            return df
        elif file_format == "json":
            df = spark.read.json(read_file_path, multiLine=True)
            return df
        elif file_format == "parquet":
            df = spark.read.parquet(read_file_path)
            return df
        return None
    except Exception as e:
        error_msg = EXCEPTION_ERROR_MESSAGE + str(e) + " in reading data from s3 connector"
        print(error_msg)
        mysql_connector_object.update_job_log_on_error(error_msg)
        return None


def write_data(sc, df, file):
    try:
        sc._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", DESTINATION_S3_CONNECTOR["access_key"])
        sc._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", DESTINATION_S3_CONNECTOR["secret_key"])
        file_format = DESTINATION_S3_CONNECTOR["file_format"]
        if file == "":
            write_file_path = "s3a://" + DESTINATION_S3_CONNECTOR["bucket_name"] + "/" + DESTINATION_S3_CONNECTOR[
                'file_path']
        else:
            write_file_path = "s3a://" + DESTINATION_S3_CONNECTOR["bucket_name"] + "/" + DESTINATION_S3_CONNECTOR[
                'file_path'] + file.split(".")[0] + "." + file_format
        if file_format == "parquet":
            df.write.parquet(write_file_path)
        elif file_format == "json":
            df.write.json(write_file_path)
        elif file_format == "orc":
            df.write.format(file_format).save(write_file_path)
        else:
            return False
        return True
    except Exception as e:
        error_msg = EXCEPTION_ERROR_MESSAGE + str(e) + " in writing data to s3 connector"
        print(error_msg)
        mysql_connector_object.update_job_log_on_error(error_msg)
        return False


def get_data_size():
    '''
    Fetch size of source data file from S3 bucket
    '''
    try:
        session = boto3.session.Session(aws_access_key_id=SOURCE_S3_CONNECTOR["access_key"],
                                        aws_secret_access_key=SOURCE_S3_CONNECTOR["secret_key"])
        s3 = session.resource('s3')
        my_bucket = s3.Bucket(SOURCE_S3_CONNECTOR['bucket_name'])
        for objs in my_bucket.objects.filter(Prefix=SOURCE_S3_CONNECTOR['file_path']):
            # Bytes to MB file size conversion
            data_size = objs.size / (1024 * 1024)
        return data_size
    except Exception as e:
        error_msg = EXCEPTION_ERROR_MESSAGE + str(e) + " in getting data size"
        print(error_msg)
        mysql_connector_object.update_job_log_on_error(error_msg)
        return None


def get_file_list_and_size():
    '''
    Fetch size of source data file and list of files from S3 bucket
    '''
    try:
        session = boto3.session.Session(aws_access_key_id=SOURCE_S3_CONNECTOR["access_key"],
                                        aws_secret_access_key=SOURCE_S3_CONNECTOR["secret_key"])
        s3 = session.resource('s3')
        my_bucket = s3.Bucket(SOURCE_S3_CONNECTOR['bucket_name'])
        file_prefix = SOURCE_S3_CONNECTOR['file_path'].split("/")
        file_prefix = file_prefix[file_prefix.__len__() - 2]
        objects = my_bucket.objects.filter(Prefix=file_prefix)
        my_list = list()
        total_size = 0
        for obj in objects:
            total_size = total_size + obj.size
            if SOURCE_S3_CONNECTOR['file_format'] in str(obj.key) and str(obj.key).count("/") == 1:
                # For avoiding other format files and sub directories
                my_list.append(str(obj.key).split("/")[1])
        # Bytes to MB file size conversion
        total_size = total_size / (1024 * 1024)
        return my_list, total_size
    except Exception as e:
        error_msg = EXCEPTION_ERROR_MESSAGE + str(e) + " in getting list of files and data size"
        print(error_msg)
        mysql_connector_object.update_job_log_on_error(error_msg)
        return None, None



