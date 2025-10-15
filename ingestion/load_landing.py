import sys, os


project_root = os.path.abspath("/u01/datle/project_root")

# Thêm vào PYTHONPATH
if project_root not in sys.path:
    sys.path.append(project_root)
from common.db_utils import DBHelper
from common.s3_utils import S3Connector
from common.spark_utils import SparkConnector
from utils_ingest import *



if __name__ == "__main__":
    path='/u01/datle/project_root/configs/ingestion_config.yaml'
    db = DBHelper(path,'data_source_1')
    s3 = S3Connector(path,'data_lake_aws')
    spark = SparkConnector(s3_config='data_lake_aws', path=path)
    spark_session = spark.connect()
    bucket = 'covid192406'
    source_table1='covid19.covid2'
    source_table2='covid19.dedupled_isocode'
    source_table3='covid19.dim_country_code'
    target_table1='bronze_zone/covid1'
    target_table2='bronze_zone/dedupled_isocode'
    target_table3='bronze_zone/dim_country_code'


    incremental_query = checking_latest_date(spark_session, s3, target_table1, source_table1)
    incremental_df = db.read_postgres_with_spark(spark = spark_session, query = incremental_query)
    print(incremental_query)
    print('insert rows:',incremental_df.count())
    if incremental_df.count() == 0:
        print('data is up-to-date')
    else:
        s3.write_parquet_spark(incremental_df, bucket, target_table1, mode="append", partition_col=None)

    preprocess_landing(spark_session, db, s3, bucket, source_table2, target_table2)
    # preprocess_landing(spark_session, db, s3, bucket, source_table3, target_table3)


    

    



