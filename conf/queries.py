# Import Conf Package
from conf.constants import *

SELECT_SPARK_JOB_DATA_ID = "SELECT max(job_id) FROM "+MYSQL_CONFIG['mysql_dbname']+ ".`spark_job_data`"

INSERT_INTO_SPARK_JOB_DATA = "INSERT INTO "+MYSQL_CONFIG['mysql_dbname']+ ".`spark_job_data` (`source_s3_bucket_name`," \
                             "`source_file_name`,`source_file_format`,`destination_s3_bucket_name`," \
                             "`destination_file_name`,`destination_file_format`) values " \
                             "('%s', '%s', '%s', '%s', '%s', '%s');"

INSERT_INTO_SPARK_JOB_LOGS = "INSERT INTO "+MYSQL_CONFIG['mysql_dbname']+ ".`spark_job_logs` (`job_id`," \
                             "`spark_configuration`,`source_file_data_size_mb`,`job_status`) values " \
                             "(%d, '%s',%f , '%s');"

UPDATE_SPARK_JOB_LOGS_STATUS_AND_TIME = "UPDATE "+MYSQL_CONFIG['mysql_dbname']+".`spark_job_logs`" \
                                                                               " set `job_status` = 'Completed',"\
                                                                               "`time_taken` = '%s' "\
                                                                               "where `job_id` = %d ;"

UPDATE_SPARK_JOB_LOGS_ERROR_MSG = "UPDATE "+MYSQL_CONFIG['mysql_dbname']+ ".`spark_job_logs` set `error_message` = '%s'," \
                                                                          "`job_status` = 'Error' " \
                                                                          "where `job_id` = %d ;"