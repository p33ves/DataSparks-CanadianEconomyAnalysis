import os
import sys
import boto3

from pyspark.sql import SparkSession

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
spark = SparkSession.builder.appName('statcan data cleanse + schema creation').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

IN_PATH = "s3://mysparks/data/raw/statcan/"
OUT_PATH = "s3://mysparks/data/clean/statcan/"
SCHEMA_PATH = "s3://mysparks/schema/statcan/"
s3_obj = boto3.client('s3')
# os.makedirs(SCHEMA_PATH, exist_ok=True)
# os.makedirs(OUT_PATH, exist_ok=True)


def clean_csv(file_name):
    table_id = os.path.basename(file_name).split('.')[0]
    try:
        input_data = spark.read.option("header", "true") \
            .csv(file_name, inferSchema=True)
        data_in_range = input_data.where(
            input_data['REF_DATE'].startswith('201') |
            input_data['REF_DATE'].startswith('202'))
        # if table_id == "12100121":
        #     output_data = data_in_range.filter(data_in_range["Seasonal adjustment"] == "Seasonally adjusted") \
        #         .coalesce(1)
        # elif table_id == "14100287":
        #     output_data = data_in_range.filter(data_in_range["Data type"] == "Seasonally adjusted").coalesce(1)
        # elif table_id == "20100008":
        #     output_data = data_in_range.filter(data_in_range["Adjustments"] == "Seasonally adjusted").coalesce(1)
        # else:
        output_data = data_in_range.coalesce(1)
        output_data.write \
            .option("header", "true") \
            .csv(OUT_PATH + table_id)
        os.remove(file_name)
        s3_obj.put_object(Body=input_data.schema.json(),
                          Bucket='mysparks', Key=SCHEMA_PATH + table_id + ".json")
        return {table_id: "Successful"}
    except Exception as err:
        return {table_id: err}


if __name__ == "__main__":
    table_list = open('s3://mysparks/statcan_url_list.txt', 'r').readlines()
    input_files = list(map(lambda x: IN_PATH + x.split(':')[1].strip() + ".csv", table_list))
    results = list(map(clean_csv, input_files))
    print(results)
