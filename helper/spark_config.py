# Import Conf Package
from conf.responses import *

from pyspark.sql import SparkSession



def get_spark_session(data_size):
  '''
  Initialize and return spark session based on source file data size.
  '''
  try:
    executor_memory_gb="7G"
    if data_size <2048:
      executor_memory_gb = "2G"
    elif data_size < 5120:
      executor_memory_gb = "3G"
    elif data_size <10240:
      executor_memory_gb = "5G"
    spark = SparkSession \
      .builder \
      .appName("Transfer s3 to another s3") \
      .config("spark.dynamicAllocation.enabled", "true") \
      .config("spark.dynamicAllocation.minExecutors", "2") \
      .config("spark.dynamicAllocation.maxExecutors", "2") \
      .config("spark.executor.cores", 3) \
      .config("spark.executor.memory", executor_memory_gb) \
      .getOrCreate()
    return spark
  except Exception as e:
    error_msg = EXCEPTION_ERROR_MESSAGE+str(e)+" in getting spark session"
    print(error_msg)
    return None

def get_spark_config(sc):
  '''
  Return spark config properties in type dictionary.
  '''
  try:
    spark_conf_properties= dict()
    spark_conf_properties['spark.executor.cores']=sc.getConf().get('spark.executor.cores')
    spark_conf_properties['spark.executor.memory']=sc.getConf().get('spark.executor.memory')
    spark_conf_properties['spark.driver.memory']=sc.getConf().get('spark.driver.memory')
    return spark_conf_properties
  except Exception as e:
    error_msg = EXCEPTION_ERROR_MESSAGE + str(e) + " in getting spark "
    print(error_msg)
    return None