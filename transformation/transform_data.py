import sys, os


project_root = os.path.abspath("/u01/datle/project_root")

# Thêm vào PYTHONPATH
if project_root not in sys.path:
    sys.path.append(project_root)
from common.db_utils import DBHelper
from common.s3_utils import S3Connector
from common.spark_utils import SparkConnector
from dags.utils_dags import *
from datetime import datetime, timedelta

if __name__ == "__main__":
    today,yesterday = get_execution_date()
    # print(yesterday.strftime('%Y-%m-%d'))
    path='/u01/datle/project_root/configs/ingestion_config.yaml'
    db = DBHelper(path,'data_source_1')
    s3 = S3Connector(path,'data_lake_aws')
    spark = SparkConnector(s3_config='data_lake_aws', path=path)
    spark_session = spark.connect()
    bucket = 'covid192406'
    source_table1='silver_zone/covid1'
    source_table2='bronze_zone/dedupled_isocode'
    source_table3='bronze_zone/dim_country_code'
    source_table4='covid19.fact_covid_4'
    target_table1='gold_zone/fact_covid'
    target_table2='gold_zone/dim_country_scd'

    # process dim data
    query = """
        with with_previous as (
        select  
        date,
        iso_code,
        continent,
        location,
        population,
        lag(population,1) over (partition by iso_code order by date) as previous_population,
        population_density,
        lag(population_density,1) over (partition by iso_code order by date) as previous_population_density,
        median_age,
        lag(median_age,1) over (partition by iso_code order by date) as previous_median_age,
        gdp_per_capita,
        lag(gdp_per_capita,1) over (partition by iso_code order by date) as previous_gdp_per_capita,
        life_expectancy,
        lag(life_expectancy,1) over (partition by iso_code order by date) as previous_life_expectancy
        from dim_country_code --where extract(year from date) <= 2021
        )
        , with_indicator as (
        select
        *,
        case when population <> previous_population then 1
            when population_density <> previous_population_density then 1
            when median_age <> previous_median_age then 1
            when gdp_per_capita <> previous_gdp_per_capita then 1
            when life_expectancy <> previous_life_expectancy then 1
            else 0 end as change_indicator
        from with_previous),
        with_streak as (
        select 
        *,
        sum(change_indicator) over (partition by iso_code order by date) as streak_indentifier
        from with_indicator
        )
        select
        iso_code,
        continent,
        location,
        population,
        population_density,
        median_age, 
        gdp_per_capita,
        life_expectancy,
        min(date) as start_date,
        max(date) as end_date,
        CURRENT_DATE as current_year
        from with_streak
        group by iso_code,continent,location,population,population_density,median_age, gdp_per_capita,life_expectancy
        """
    cleaned_data3 = s3.read_parquet_spark(spark_session, bucket, source_table3)
    dim_country_scd_df = s3.query_data_spark(spark_session, [('dim_country_code',cleaned_data3)], query)
    # target_table='/covid1_data'

    # query = f'SELECT * FROM {source_table1} limit 100'
    # source_df = db.execute_query(query, fetch=True)
    # target_df = s3.read_parquet_spark(spark_sesssion,'covid192406',target_table)
    cleaned_data1 = s3.read_parquet_spark(spark_session, bucket, source_table1)
    cleaned_data2 = s3.read_parquet_spark(spark_session, bucket, source_table2)
    

    # query = f'(SELECT * FROM {source_table4} where dim_date=({yesterday})) as t'
    query =f"(select * from {source_table4} where dim_date=DATE('{yesterday}')) as t"
    yesterday_df = db.read_postgres_with_spark(spark = spark_session, query = query)


    table_lists = [('temp',cleaned_data1),('temp1',cleaned_data2)]

    query = f"""
        SELECT 
        t.iso_code,
        t.date,
        t.icu_patients,
        t.stringency_index,
        t.reproduction_rate,
        t.total_cases,
        t.total_deaths,
        t.total_tests
    FROM temp t
    INNER JOIN temp1 c
        ON t.iso_code = c.iso_code
    """
    trs_df1 = s3.query_data_spark(spark_session, table_lists, query)
    query = """
        with r1 as (
        select
        coalesce(t.iso_code,y.dim_iso_code) as dim_iso_code,
        coalesce(t.date, DATE_ADD(y.dim_date, 1)) as dim_date,
        case when coalesce(t.total_cases, y.m_total_cases) < y.m_total_cases then y.m_total_cases else coalesce(t.total_cases, y.m_total_cases) end as m_total_cases,
        case when coalesce(t.total_cases, y.m_total_cases) < y.m_total_cases then y.m_total_cases else coalesce(t.total_cases, y.m_total_cases) end - coalesce(y.m_total_cases,0) as m_new_cases,
        case when coalesce(t.total_deaths, y.m_total_deaths) < y.m_total_deaths then y.m_total_deaths else coalesce(t.total_deaths, y.m_total_deaths) end as m_total_deaths,
        case when coalesce(t.total_deaths, y.m_total_deaths) < y.m_total_deaths then y.m_total_deaths else coalesce(t.total_deaths, y.m_total_deaths) end - coalesce(y.m_total_deaths,0) as m_new_deaths,
        case when coalesce(t.icu_patients, y.m_total_icu_patients) < y.m_total_icu_patients then y.m_total_icu_patients else coalesce(t.icu_patients, y.m_total_icu_patients) end as m_total_icu_patients,
        case when coalesce(t.icu_patients, y.m_total_icu_patients) < y.m_total_icu_patients then y.m_total_icu_patients else coalesce(t.icu_patients, y.m_total_icu_patients) end - coalesce(y.m_total_icu_patients,0) as m_new_icu_patients,
        case when coalesce(t.total_tests,y.m_total_tests) < y.m_total_tests then y.m_total_tests else coalesce(t.total_tests,y.m_total_tests) end as m_total_tests,
        case when coalesce(t.total_tests,y.m_total_tests) < y.m_total_tests then y.m_total_tests else coalesce(t.total_tests,y.m_total_tests) end - coalesce(y.m_total_tests,0) as m_new_tests,
        coalesce(t.reproduction_rate, y.m_reproduction_rate) as m_reproduction_rate,
        coalesce(t.stringency_index,y.m_stringency_index) as m_stringency_index
        from cleaned_today t full outer join yesterday y on t.iso_code = y.dim_iso_code)
        select * from r1
        """
    table_lists = [('cleaned_today',trs_df1),('yesterday',yesterday_df)]
    
    final_df = s3.query_data_spark(spark_session, table_lists, query)

    s3.write_parquet_spark(final_df, bucket, target_table1, mode="overwrite", partition_col=None)
    s3.write_parquet_spark(dim_country_scd_df, bucket, target_table2, mode="overwrite", partition_col=None)

