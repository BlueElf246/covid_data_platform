import sys, os


project_root = os.path.abspath("/u01/datle/project_root")

# Thêm vào PYTHONPATH
if project_root not in sys.path:
    sys.path.append(project_root)
from common.db_utils import DBHelper
from common.s3_utils import S3Connector
from common.spark_utils import SparkConnector
# from transformation.utils_transform import *
from dags.utils_dags import *

if __name__ == "__main__":
    today,yesterday = get_execution_date()
    path='/u01/datle/project_root/configs/ingestion_config.yaml'
    db = DBHelper(path,'data_source_1')
    s3 = S3Connector(path,'data_lake_aws')
    spark = SparkConnector(s3_config='data_lake_aws', path=path)
    spark_sesssion = spark.connect()
    bucket = 'covid192406'
    source_table1='bronze_zone/covid1'
    target_table='silver_zone/covid1'
    # target_table='/covid1_data'

    # query = f'SELECT * FROM {source_table1} limit 100'
    # source_df = db.execute_query(query, fetch=True)
    # target_df = s3.read_parquet_spark(spark_sesssion,'covid192406',target_table)
    raw_data = s3.read_parquet_spark(spark_sesssion, bucket, source_table1)
    query = f"""with today as (select 
        *,
        row_number() over(partition by iso_code order by date) as row_num 
        from temp where date = DATE('{today}'))
        select * from today t where row_num=1
        """
    dedupled_df = s3.query_data_spark(spark_sesssion, [('temp',raw_data)], query)
    # print(dedupled_df.count())
    query = f"""
        SELECT 
        t.iso_code,
        CAST(t.date AS DATE) AS date,
        COALESCE(t.icu_patients, 0) AS icu_patients,
        COALESCE(t.stringency_index, -1) AS stringency_index,
        CASE 
            WHEN t.reproduction_rate IS NULL OR t.reproduction_rate < 0 THEN 0 
            ELSE t.reproduction_rate 
        END AS reproduction_rate,
        COALESCE(t.total_cases, 0) AS total_cases,
        COALESCE(t.total_deaths, 0) AS total_deaths,
        COALESCE(t.total_tests, 0) AS total_tests
    FROM temp t
        """
    cleaned_df = s3.query_data_spark(spark_sesssion, [('temp',dedupled_df)], query)
    # target_df = s3.read_parquet_spark(spark_sesssion,'covid192406',target_table)
    # print(cleaned_df.printSchema())
    cleaned_today_covid_fct = cleaned_df \
        .withColumn("iso_code", cleaned_df["iso_code"].cast("string")) \
        .withColumn("date", cleaned_df["date"].cast("date")) \
        .withColumn("total_cases", cleaned_df["total_cases"].cast("double")) \
        .withColumn("total_deaths", cleaned_df["total_deaths"].cast("double")) \
        .withColumn("icu_patients", cleaned_df["icu_patients"].cast("double")) \
        .withColumn("reproduction_rate", cleaned_df["reproduction_rate"].cast("float")) \
        .withColumn("stringency_index", cleaned_df["stringency_index"].cast("float")) \
        .withColumn("total_tests", cleaned_df["total_tests"].cast("double"))
    # print(cleaned_today_covid_fct.printSchema())
    s3.write_parquet_spark(cleaned_today_covid_fct, bucket, target_table, mode="overwrite", partition_col=None)
    # print(len(spark_sesssion))
    # print(len(target_df))