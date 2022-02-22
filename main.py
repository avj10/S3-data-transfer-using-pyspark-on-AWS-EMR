# Import Conf Package
from conf.responses import *

# Import helper Package
from helper.validate import *
from helper.transfer import *

from pyspark.sql import SparkSession


def main():
    '''
    Main function which call validation and transfer data fun from S3 to S3 connector.
    '''

    try:
        if validate_source_and_destination_s3_connector():
            if not transfer_data():
                print(ERROR_MESSAGE + "Transferring of data failed")
    except Exception as e:
        print(EXCEPTION_ERROR_MESSAGE+ str(e) + " in main operation")

if __name__ == '__main__':
    main()
