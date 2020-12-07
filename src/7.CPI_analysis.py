import os
import json
import sys
import datetime
import boto3
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, types
from pyspark.sql.functions import to_date, lit, year, avg, round

IN_PATH = "s3://mysparks/data/clean/statcan/"
OUT_PATH = "s3://mysparks/OUTPUT-Folder/"
#IN_PATH = "../data/clean/statcan/"
#OUT_PATH = "../OUTPUT-Folder/"

cpi_id = "18100004"
SCHEMA_PATH = "schema/statcan/" + cpi_id + ".json"

s3_obj = boto3.client('s3')
s3_cpi_obj = s3_obj.get_object(Bucket='mysparks', Key=SCHEMA_PATH)
s3_cpi_data = s3_cpi_obj['Body'].read().decode('utf-8')
cpi_schema = json.loads(s3_cpi_data)

os.makedirs(OUT_PATH, exist_ok=True)
#cpi_schema = json.load(open("../schema/statcan/" + cpi_id + ".json"))

def main():
    cpi_df = spark.read.csv(IN_PATH + cpi_id + '/*.csv',
                            schema=types.StructType.fromJson(cpi_schema))

    # filter out null values for required columns
    notnull_df = cpi_df.filter(cpi_df['REF_DATE'].isNotNull() | cpi_df['GEO'].isNotNull() | cpi_df['VALUE'].isNotNull())

    # filter out "All-items" only from 'Products and product groups' columns
    allitems_df = notnull_df.filter(notnull_df['Products and product groups'] == lit('All-items'))

    # convert 'REF_DATE' to date type
    date_df = allitems_df.withColumn('REF_DATE', to_date(allitems_df['REF_DATE'], 'yyyy-MM'))

    # fetch data for the last 10 years
    decade_df = date_df.where(date_df['REF_DATE'].between(
        datetime.datetime.strptime('2010-01-01', '%Y-%m-%d'), datetime.datetime.strptime('2020-10-01', '%Y-%m-%d')))

    # Taking only the provinces
    province_df = decade_df.filter(~decade_df['GEO'].contains(','))

    # take the yearly avg of cpi values for each province and restructure the dataframe based on provinces
    result_cpi_df = province_df.groupby(year('REF_DATE').alias('YEAR')).pivot('GEO').agg(
        round(avg('VALUE'), 2)).orderBy('YEAR')

    with open(SCHEMA_PATH + "cpi.json", 'w') as out_file:
        out_file.write(result_cpi_df.schema.json())
    result_cpi_df.coalesce(1).write.csv(OUT_PATH + 'Canada_CPI_output', header='true', mode='overwrite')


if __name__ == '__main__':
    spark = SparkSession.builder.appName('CPI Analysis').getOrCreate()
    assert spark.version >= '2.4'  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main()
