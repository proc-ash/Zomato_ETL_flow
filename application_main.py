import sys
from lib import DataConverter, DataManipulation, DataReader, Utils, DataTransformation
from pyspark.sql.functions import *

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Please specify the environment")
        sys.exit(-1)
    job_run_env = sys.argv[1]
    print("Creating Spark Session")
    spark = Utils.get_spark_session(job_run_env)
    print("Created Spark Session")
    DataConverter.convert_data(spark,job_run_env)
    raw_data = DataReader.clean_raw_data(spark,job_run_env)
    cleaned_data = DataManipulation.clean_data(raw_data, job_run_env)
    DataTransformation(cleaned_data)
    print("end of main")