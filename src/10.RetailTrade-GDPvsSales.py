import sys
import datetime
import json
import os

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import to_date, lit, year, avg, round

# Comparing the Retail Trade Sales vs its GDP over the last 10 years

IN_PATH = "s3://mysparks/data/clean/statcan/"
OUT_PATH = "s3://mysparks/OUTPUT-Folder/"
SCHEMA_PATH = "s3://mysparks/schema/statcan/"
gdp_id = "36100434"
trade_id = "20100008"
os.makedirs(OUT_PATH, exist_ok=True)
gdp_schema = json.load(open(SCHEMA_PATH + gdp_id + ".json"))
trade_sales_schema = json.load(open(SCHEMA_PATH + trade_id + ".json"))


def main():
    ########################Processing GDP Data##################################
    gdp_df = spark.read.csv(IN_PATH + gdp_id + '/*.csv',
                            schema=types.StructType.fromJson(gdp_schema))

    # filter out null values for required columns
    gdp_notnull_df = gdp_df.filter(
        gdp_df['REF_DATE'].isNotNull() | gdp_df['GEO'].isNotNull() | gdp_df['VALUE'].isNotNull())

    # filter out "Seasonally adjusted at annual rates" values only from 'Seasonal adjustment' column
    gdp_seasonal_df = gdp_notnull_df.filter(
        gdp_notnull_df['Seasonal adjustment'] == lit('Seasonally adjusted at annual rates'))

    # fetch only required columns
    gdp = gdp_seasonal_df.select('REF_DATE',
                                 (gdp_seasonal_df['North American Industry Classification System (NAICS)']).alias(
                                     'NAICS'), (gdp_seasonal_df['VALUE']).alias('GDP_VALUE'))

    # Get only "Retail Trade" industry from 'NAICS' column
    # exclude 'Retail trade (except unlicensed cannabis) [4AZ]' values
    gdp_retail = gdp.filter(gdp['NAICS'].contains('Retail trade') & (~gdp['NAICS'].contains('(')))

    # convert 'REF_DATE' to date type
    gdp_date_df = gdp_retail.withColumn('REF_DATE', to_date(gdp_retail['REF_DATE'], 'yyyy-MM'))

    # fetch data for the last 10 years
    gdp_decade_df = gdp_date_df.where(
        gdp_date_df['REF_DATE'].between(datetime.datetime.strptime('2010-01-01', '%Y-%m-%d'),
                                        datetime.datetime.strptime('2020-10-01', '%Y-%m-%d')))

    # convert 'GDP_VALUE' to int type
    gdp_int_df = gdp_decade_df.withColumn('GDP_VALUE', (gdp_decade_df['GDP_VALUE']).cast(types.IntegerType()))

    # Get yearly average data for Retail Trade GDP
    gdp_avg = gdp_int_df.groupBy(year('REF_DATE').alias('YEAR')).agg(
        round(avg(gdp_int_df['GDP_VALUE']), 2).alias('RETAIL_GDP*(10^3)'))

    ##############Processing Retail Trade Sales Data#####################################
    trade_sales_df = spark.read.csv(IN_PATH + trade_id + '/*.csv',
                                    schema=types.StructType.fromJson(trade_sales_schema))

    # Filter Null rows if fields 'REF_DATE','GEO' or 'VALUE' is Null
    trade_notnull_df = trade_sales_df.filter(
        trade_sales_df['REF_DATE'].isNotNull() | trade_sales_df['GEO'].isNotNull() | trade_sales_df[
            'VALUE'].isNotNull())

    # convert 'REF_DATE' to date type
    trade_date_df = trade_notnull_df.withColumn('REF_DATE', to_date(trade_notnull_df['REF_DATE'], 'yyyy-MM'))

    # fetch data for the last 10 years
    trade_decade_df = trade_date_df.where(
        trade_date_df['REF_DATE'].between(datetime.datetime.strptime('2010-01-01', '%Y-%m-%d'),
                                          datetime.datetime.strptime('2020-10-01', '%Y-%m-%d')))

    # filter 'GEO' values for 'Canada
    trade_date_df = trade_decade_df.filter(trade_decade_df['GEO'] == lit('Canada'))
    # filter 'North American Industry Classification System (NAICS)' with 'Retail trade [44-45]'
    trade_date_df = trade_date_df.filter(
        trade_date_df['North American Industry Classification System (NAICS)'] == lit('Retail trade [44-45]'))
    # filter 'Adjustments' with only 'Seasonally adjusted' values
    trade_date_df = trade_date_df.filter(trade_date_df['Adjustments'] == lit('Seasonally adjusted'))

    # fetch only required columns
    trade_sales = trade_date_df.select('REF_DATE', (trade_date_df['VALUE']).alias('RETAIL_TRADE_SALES'))

    # convert 'GDP_VALUE' to int type
    trade_int_df = trade_sales.withColumn('RETAIL_TRADE_SALES',
                                          (trade_sales['RETAIL_TRADE_SALES']).cast(types.IntegerType()))

    # Get yearly average data for Retail Trade GDP
    trade_avg = trade_int_df.groupBy(year('REF_DATE').alias('YEAR')).agg(
        avg(trade_int_df['RETAIL_TRADE_SALES']).alias('RETAIL_TRADE_SALES *(10^6)'))

    ###Merge Retail Trade GDP and Sales values
    gdp_sales = gdp_avg.join(trade_avg, ['YEAR']).orderBy('YEAR')

    gdp_sales.write.csv(OUT_PATH + 'Retail_Trade_output', header='true', mode='overwrite')


if __name__ == '__main__':
    spark = SparkSession.builder.appName('Retail Trade Analysis').getOrCreate()
    assert spark.version >= '2.4'  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main()
