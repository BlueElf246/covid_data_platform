import sys, os


project_root = os.path.abspath("/u01/datle/project_root")

# Thêm vào PYTHONPATH
if project_root not in sys.path:
    sys.path.append(project_root)
from common.db_utils import DBHelper
from common.s3_utils import S3Connector
from common.spark_utils import SparkConnector
# from utils_ingest import *
from utils_load import get_execuation_date

if __name__ == "__main__":
    today,yesterday = get_execuation_date()
    path='/u01/datle/project_root/configs/ingestion_config.yaml'
    db = DBHelper(path,'data_source_1')
    s3 = S3Connector(path,'data_lake_aws')
    spark = SparkConnector(s3_config='data_lake_aws', path=path)
    spark_sesssion = spark.connect()
    bucket = 'covid192406'
    source_table1='gold_zone/fact_covid'
    source_table2='gold_zone/dim_country_scd'
    target_table1='fact_covid_4'
    target_table2='dim_country_code_scd_4'

    src_table1 = s3.read_parquet_spark(spark_sesssion, bucket, source_table1)
    src_table2 = s3.read_parquet_spark(spark_sesssion, bucket, source_table2)

    db.execute_query(f"delete from {target_table1} where dim_date=DATE('{yesterday}')", params=None, fetch=False)
    db.write_to_postgres_with_spark(src_table1, target_table1, mode="append")
    db.write_to_postgres_with_spark(src_table2, target_table2, mode="overwrite")



