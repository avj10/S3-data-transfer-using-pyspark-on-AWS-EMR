# Import Conf Package
from conf.constants import *
from conf.responses import *
from conf.queries import *

import mysql.connector
import json,time

class MysqlConnector(object):
  '''
  This class used to connect mySQL server and provide the methods to insert and update database.
  '''
  def __init__(self):
    self.my_db = mysql.connector.connect(
      host=MYSQL_CONFIG['mysql_hostname'],
      user=MYSQL_CONFIG['mysql_username'],
      password=MYSQL_CONFIG['mysql_password'],
      database=MYSQL_CONFIG['mysql_dbname']
    )
    self.my_cursor = self.my_db.cursor()

    # Create required tables if not exit in database.

    # my_cursor.execute("CREATE IF NOT EXIST DATABASE usecase1")

    self.my_cursor.execute(
      "CREATE TABLE IF NOT EXISTS spark_job_data (job_id INT NOT NULL AUTO_INCREMENT, source_s3_bucket_name VARCHAR(50), "
      "source_file_name VARCHAR(50), source_file_format VARCHAR(10), destination_s3_bucket_name VARCHAR(50), "
      "destination_file_name VARCHAR(50), destination_file_format VARCHAR(10), PRIMARY KEY (`job_id`))")

    self.my_cursor.execute(
      "CREATE TABLE IF NOT EXISTS spark_job_logs (log_id INT NOT NULL AUTO_INCREMENT, job_id INT, spark_configuration "
      "VARCHAR(150), start_timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, source_file_data_size_mb FLOAT(10,"
      "2), time_taken VARCHAR(8) DEFAULT NULL, error_message VARCHAR(200) DEFAULT NULL, job_status VARCHAR(10), "
      "PRIMARY KEY (`log_id`), FOREIGN KEY (`job_id`) REFERENCES `spark_job_data`(`job_id`))")


  def insert_spark_job_data(self):
    try:
      insert_spark_job_data_query = INSERT_INTO_SPARK_JOB_DATA % (SOURCE_S3_CONNECTOR['bucket_name'],
                                                                  SOURCE_S3_CONNECTOR['file_path'],
                                                                  SOURCE_S3_CONNECTOR['file_format'],
                                                                  DESTINATION_S3_CONNECTOR['bucket_name'],
                                                                  DESTINATION_S3_CONNECTOR['file_path'],
                                                                  DESTINATION_S3_CONNECTOR['file_format'])
      self.my_cursor.execute(insert_spark_job_data_query)
      self.my_db.commit()
      if self.my_cursor.rowcount > 0:
        return True
      else:
        return False
    except Exception as e:
      error_msg = EXCEPTION_ERROR_MESSAGE + str(e) + " in inserting spark job log"
      print(error_msg)
      self.update_job_log_on_error(error_msg)
      return False

  def get_current_job_id(self):
    try:
      self.my_cursor.execute(SELECT_SPARK_JOB_DATA_ID)
      current_job_id = self.my_cursor.fetchall()[0][0]
      return current_job_id
    except Exception as e:
      error_msg = EXCEPTION_ERROR_MESSAGE + str(e) + " in inserting spark job log"
      print(error_msg)
      self.update_job_log_on_error(error_msg)
      return None

  def insert_spark_job_log(self,current_job_id, spark_conf_properties, data_size):
    try:
      spark_conf_properties = json.dumps(spark_conf_properties)
      insert_spark_job_log_query = INSERT_INTO_SPARK_JOB_LOGS % (current_job_id, spark_conf_properties, data_size,
                                                                 "Running")
      self.my_cursor.execute(insert_spark_job_log_query)
      self.my_db.commit()
      if self.my_cursor.rowcount > 0:
        return True
      else:
        return False
    except Exception as e:
      error_msg = EXCEPTION_ERROR_MESSAGE + str(e) + " in inserting spark job log"
      print(error_msg)
      self.update_job_log_on_error(error_msg)
      return False

  def update_job_log_on_error(self,error_msg):
    '''
    Open when exception occur in code update job log status to 'Error' and insert error message.
    '''
    try:
      # escaping single quote in mySQL update query
      error_msg = error_msg.replace("'", "''")
      self.my_cursor.execute(SELECT_SPARK_JOB_DATA_ID)
      current_job_id = self.my_cursor.fetchall()[0][0]
      update_spark_job_logs_error_msg = UPDATE_SPARK_JOB_LOGS_ERROR_MSG % (error_msg, current_job_id)
      self.my_cursor.execute(update_spark_job_logs_error_msg)
      self.my_db.commit()
      if self.my_cursor.rowcount > 0:
        return True
      else:
        return False
    except Exception as e:
      error_msg = EXCEPTION_ERROR_MESSAGE + str(e) + " in updating job status to error"
      print(error_msg)
      return False


  def update_job_log_status_and_time(self,start_time):
    try:
      self.my_cursor.execute(SELECT_SPARK_JOB_DATA_ID)
      current_job_id = self.my_cursor.fetchall()[0][0]
      end_time = time.time()
      time_taken = (end_time - start_time)
      time_taken=self.seconds_to_time_format_conversion(time_taken)
      update_spark_job_logs_status_and_time = UPDATE_SPARK_JOB_LOGS_STATUS_AND_TIME % (time_taken, current_job_id)
      self.my_cursor.execute(update_spark_job_logs_status_and_time)
      self.my_db.commit()
      if self.my_cursor.rowcount > 0:
        return True
      else:
        return False
    except Exception as e:
      error_msg = EXCEPTION_ERROR_MESSAGE + str(e) + " in update job log status and time taken."
      print(error_msg)
      self.update_job_log_on_error(error_msg)
      return False

  def seconds_to_time_format_conversion(self,seconds):
    '''
    Method Converts seconds to time format 'HH:MM:SS'.
    '''
    seconds = seconds % (24 * 3600)
    hour = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    seconds %= 60
    return "%d:%02d:%02d" % (hour, minutes, seconds)