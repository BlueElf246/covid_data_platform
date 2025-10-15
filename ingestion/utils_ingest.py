def checking_latest_date(spark_sesssion, s3, target_table, src_table):

    if s3.check_exists('covid192406',target_table) == False:
        # target_df = s3.read_parquet_spark(spark_sesssion,'covid192406',target_table)
        return f'(SELECT * FROM {src_table}) as t'
    else:
        target_df = s3.read_parquet_spark(spark_sesssion,'covid192406',target_table)
        query = 'select max(date) from temp'
        latest_df = s3.query_data_spark(spark=spark_sesssion, table_list=[('temp',target_df)], query=query)
        latest_date = latest_df.first()["max(date)"].strftime("%Y-%m-%d")
        query = f"(SELECT * FROM {src_table} where date > DATE('{latest_date}')) as t"
        return query
def preprocess_landing(spark_session, db, s3, bucket, src_table, tar_table):
    query = f'(SELECT * FROM {src_table}) as t '
    df = db.read_postgres_with_spark(spark = spark_session, query = query)
    s3.write_parquet_spark(df, bucket, tar_table, mode="overwrite", partition_col=None)