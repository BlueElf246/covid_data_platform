from utils_check import *

import sys, os


project_root = os.path.abspath("/u01/datle/project_root")

# Thêm vào PYTHONPATH
if project_root not in sys.path:
    sys.path.append(project_root)
    
from common.db_utils import DBHelper
from common.s3_utils import S3Connector
from common.spark_utils import SparkConnector
from common.config import ConfigLoader
from dags.utils_dags import *

def data_quality_check(dfs, config):
    """
    dfs: list of tuples [(df, 'table_name')]
    config: dict loaded from YAML
    """
    # --- Loop through each table ---
    for df, name in dfs:
        print(f"\n🚀 Checking {name} ...")
        rules = config.get(name, {})

        check_dataframe_not_empty(df, name)
        check_no_nulls_in_critical_columns(df, rules.get("not_null_cols", []), name)

        # Unique column checks (support composite keys)
        unique_cols = rules.get("unique_col", [])
        unique_cols = [uc if isinstance(uc, list) else [uc] for uc in unique_cols]
        check_unique_ids(df, unique_cols, name)

        # Foreign key checks
        # for fk in rules.get("ref_table", []):
        #     ref_name = fk["ref_table"]
        #     ref_df = next((x[0] for x in dfs if x[1] == ref_name), None)
        #     if ref_df is not None:
        #         check_foreign_key_integrity(df, ref_df, fk["column"], fk["ref_column"], name)

        # Range checks
        # check_range_of_values(df, rules.get("value_range_cols", []), name)

    print("\n🎉 All data quality checks passed successfully!")

if __name__ == "__main__":
    today,yesterday = get_execution_date()
    path='/u01/datle/project_root/configs/ingestion_config.yaml'
    db = DBHelper(path,'data_source_1')
    s3 = S3Connector(path,'data_lake_aws')
    spark = SparkConnector(s3_config='data_lake_aws', path=path)
    spark_session = spark.connect()
    bucket = 'covid192406'
    key1 = '/gold_zone/fact_covid'
    key2 = '/gold_zone/dim_country_scd'
    df1 = s3.read_parquet_spark(spark_session, bucket, key1)
    # df1_today = df1.filter(col("dim_date") == to_date(F.lit(today)))
    df2 = s3.read_parquet_spark(spark_session, bucket, key2)
    table_list = [(df1, 'fact_covid_4'), (df2, 'dim_country_code_scd')]

    path_quality = '/u01/datle/project_root/configs/data_quality_config.yaml'

    quality_config = ConfigLoader(path)
    data_quality_check(table_list, quality_config)