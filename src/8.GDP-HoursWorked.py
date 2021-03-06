import sys
import os
import datetime
import json
import boto3
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import to_date, lit, year, avg, split

# Both the Tables have data for Canada for industries and not province-wise

IN_PATH = "s3://mysparks/data/clean/statcan/"
OUT_PATH = "s3://mysparks/OUTPUT-Folder/"
#IN_PATH = "../data/clean/statcan/"
#OUT_PATH = "../OUTPUT-Folder/"

gdp_id = "36100434"
hrs_id = "14100289"
GDP_SCHEMA_PATH = "schema/statcan/" + gdp_id + ".json"
HOURS_SCHEMA_PATH = "schema/statcan/" + hrs_id + ".json"

s3_obj = boto3.client('s3')
s3_gdp_obj = s3_obj.get_object(Bucket='mysparks', Key=GDP_SCHEMA_PATH)
s3_gdp_data = s3_gdp_obj['Body'].read().decode('utf-8')
gdp_schema = json.loads(s3_gdp_data)

s3_hours_obj = s3_obj.get_object(Bucket='mysparks', Key=HOURS_SCHEMA_PATH)
s3_hours_data = s3_hours_obj['Body'].read().decode('utf-8')
hours_schema = json.loads(s3_hours_data)

# os.makedirs(OUT_PATH, exist_ok=True)
# gdp_schema = json.load(open("../schema/statcan/" + gdp_id + ".json"))
# hours_schema = json.load(open("../schema/statcan/" + hrs_id + ".json"))

def main():
    ########################Processing GDP Data##################################
    gdp_df = spark.read.csv(IN_PATH + gdp_id + '/*.csv',
                            schema=types.StructType.fromJson(gdp_schema))

    # filter out null values for required columns
    gdp_notnull_df = gdp_df.filter(
        gdp_df['REF_DATE'].isNotNull() | gdp_df['GEO'].isNotNull() | gdp_df['VALUE'].isNotNull())

    # filter out "Trading-day adjusted" values only from 'Seasonal adjustment' column
    gdp_seasonal_df = gdp_notnull_df.filter(gdp_notnull_df['Seasonal adjustment'] == lit('Trading-day adjusted'))

    # convert 'REF_DATE' to date type
    gdp_date_df = gdp_seasonal_df.withColumn('REF_DATE', to_date(gdp_seasonal_df['REF_DATE'], 'yyyy-MM'))

    # fetch data for the last 10 years
    gdp_decade_df = gdp_date_df.where(
        gdp_date_df['REF_DATE'].between(datetime.datetime.strptime('2010-01-01', '%Y-%m-%d'),
                                        datetime.datetime.strptime('2020-10-01', '%Y-%m-%d')))

    # Elimininate from 'NAICS' column all the values in [] to get the industry names
    gdp_filter = gdp_decade_df.withColumn('North American Industry Classification System (NAICS)', (
        split(gdp_decade_df['North American Industry Classification System (NAICS)'], " \[")[0]))

    # fetch only required columns
    gdp = gdp_filter.select('REF_DATE',
                            (gdp_filter['North American Industry Classification System (NAICS)']).alias('NAICS'),
                            (gdp_filter['VALUE']).alias('GDP_VALUE'))

    ##############Processing Actual Hours Worked Data#####################################
    hours_df = spark.read.csv(IN_PATH + hrs_id + '/*.csv',
                              schema=types.StructType.fromJson(hours_schema))

    # Filter Null rows if fields 'REF_DATE','GEO' or 'VALUE' is Null
    hours_notnull_df = hours_df.filter(
        hours_df['REF_DATE'].isNotNull() | hours_df['GEO'].isNotNull() | hours_df['VALUE'].isNotNull())

    # convert 'REF_DATE' to date type
    hours_date_df = hours_notnull_df.withColumn('REF_DATE', to_date(hours_notnull_df['REF_DATE'], 'yyyy-MM'))

    # fetch data for the last 10 years
    hours_decade_df = hours_date_df.where(
        hours_date_df['REF_DATE'].between(datetime.datetime.strptime('2010-01-01', '%Y-%m-%d'),
                                          datetime.datetime.strptime('2020-10-01', '%Y-%m-%d')))

    # Fetch only the 'Estimate' values for hours and ignore all standard error values
    hours_est = hours_decade_df.where(hours_decade_df['Statistics'] == lit('Estimate'))

    # Elimininate from 'NAICS' column all the values in [] to spearate out the industry names
    hours_filter = hours_est.withColumn('North American Industry Classification System (NAICS)', (
        split(hours_est['North American Industry Classification System (NAICS)'], " \[")[0]))

    # fetch only required columns
    hours = hours_filter.select('REF_DATE',
                                (hours_filter['North American Industry Classification System (NAICS)']).alias('NAICS'),
                                (hours_filter['VALUE']).alias('HOURS_WORKED'))

    #############Converting NAICS into Indutry values and filtering out major industries.
    ###GDP table
    gdp_rep = gdp.withColumn('NAICS', functions.regexp_replace('NAICS', r'(Wholesale|Retail trade)',
                                                               'Wholesale and retail trade')) \
        .withColumn('NAICS',
                    functions.regexp_replace('NAICS', r'(Finance and insurance|Real estate and rental and leasing)',
                                             'Finance, insurance, real estate, rental and leasing')) \
        .withColumn('NAICS', functions.regexp_replace('NAICS', r'(Arts, entertainment and recreation)',
                                                      'Entertainment and recreation')) \
        .withColumn('NAICS', functions.regexp_replace('NAICS',
                                                      r'(Business sector industries|Business sector, goods|Business sector, services)',
                                                      'Business')) \
        .withColumn('NAICS', functions.regexp_replace('NAICS', r'(Agriculture, forestry, fishing and hunting)',
                                                      'Agriculture, forestry, fishing, mining, quarrying, oil and gas')) \
        .withColumn('NAICS', functions.regexp_replace('NAICS', r'(Mining, quarrying, and oil and gas extraction)',
                                                      'Agriculture, forestry, fishing, mining, quarrying, oil and gas'))

    ###Hours Worked table
    hours_rep = hours.withColumn('NAICS', functions.regexp_replace('NAICS', r'(Agriculture)',
                                                                   'Agriculture, forestry, fishing, mining, quarrying, oil and gas')) \
        .withColumn('NAICS', functions.regexp_replace('NAICS', r'(Forestry, fishing, mining, quarrying, oil and gas)',
                                                      'Agriculture, forestry, fishing, mining, quarrying, oil and gas')) \
        .withColumn('NAICS',
                    functions.regexp_replace('NAICS', r'(Total actual hours worked, all industries)', 'All industries')) \
        .withColumn('NAICS',
                    functions.regexp_replace('NAICS', r'(Business, building and other support services)', 'Business')) \
        .withColumn('NAICS', functions.regexp_replace('NAICS', r'(Information, culture and recreation)',
                                                      'Entertainment and recreation')) \
        .withColumn('NAICS',
                    functions.regexp_replace('NAICS', r'(Goods-producing sector)', 'Goods-producing industries')) \
        .withColumn('NAICS',
                    functions.regexp_replace('NAICS', r'(Services-producing sector)', 'Service-producing industries'))

    # convert 'HOURS_WORKED' to int type
    hours_int_df = hours_rep.withColumn('HOURS_WORKED', (hours_rep['HOURS_WORKED']).cast(types.IntegerType()))
    # Averaging the hours worked for similar industries
    hours_group = hours_int_df.groupBy('REF_DATE', 'NAICS').agg(avg('HOURS_WORKED').alias('HOURS_WORKED'))

    # convert 'GDP_VALUE' to int type
    gdp_int_df = gdp_rep.withColumn('GDP_VALUE', (gdp_rep['GDP_VALUE']).cast(types.IntegerType()))
    # Averaging the hours worked for similar industries
    gdp_group = gdp_int_df.groupBy('REF_DATE', 'NAICS').agg(avg('GDP_VALUE').alias('GDP_VALUE'))

    # Merge GDP and Actual Hours worked tables based on 'NAICS' ie. based on industry data
    gdp_hours_df = gdp_group.join(hours_group, ['REF_DATE', 'NAICS'], 'inner')

    ###Obtain Labour Productivity (GDP per hours worked)
    ## GDP is in Million Dollars unit scale(10^6), Hours Worked is in Thousands scale(10^3). Hence Labour Productivity value is obtained my multiplying GDP/Hours_Worked by 10^3. Resulting Labour prodcuctivity is in Dollars per hour.
    labour_prod = gdp_hours_df.withColumn('labour_productivity',
                                          (gdp_hours_df['GDP_VALUE'] / gdp_hours_df['HOURS_WORKED']) * 1000)

    # Get average values of annual GDP, Actual Hours Worked, and Labour Productivity, Industry-wise
    result_df = labour_prod.groupby(year('REF_DATE').alias('YEAR'), 'NAICS').agg(
        avg('HOURS_WORKED').alias('HOURS_WORKED'), avg('GDP_VALUE').alias('GDP_VALUE'),
        avg('labour_productivity').alias('labour_productivity')).orderBy('YEAR')

    result_df.write.csv(OUT_PATH + 'GDP_hours_worked_output', header='true', mode='overwrite')

    # Restructure the dataframe for yearly Labour Productivity values for 18 different industries in Canada
    labour_ip = labour_prod.groupby(year('REF_DATE').alias('YEAR')).pivot('NAICS').agg(
        avg('labour_productivity').alias('labour_productivity')).orderBy('YEAR')

    labour_ip.write.csv(OUT_PATH + 'Labour_productivity_output', header='true', mode='overwrite')


if __name__ == '__main__':
    spark = SparkSession.builder.appName('GDP vs Actual Hours Worked Analysis').getOrCreate()
    assert spark.version >= '2.4'  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main()
