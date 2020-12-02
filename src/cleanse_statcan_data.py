import os
import sys
import json
import shutil
from pyspark.sql import SparkSession, functions, types

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
spark = SparkSession.builder.appName('statcan data cleanse').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

IN_PATH = "../data/raw/statcan/"
OUT_PATH = "../data/clean/statcan/"
SCHEMA_PATH = "../schema/statcan/"
os.makedirs(OUT_PATH, exist_ok=True)


def clean_csv(file_name):
    table_id = os.path.basename(file_name).split('.')[0]
    input_schema = json.load(open(SCHEMA_PATH + table_id + ".json", 'r'))
    try:
        input_data = spark.read.option("header", "true") \
            .csv(file_name, schema=types.StructType.fromJson(input_schema))
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
        # os.remove(file_name)
        # source = [out_file for out_file in os.listdir(OUT_PATH + table_id) if out_file.endswith(".csv")][0]
        # destination = OUT_PATH + table_id + ".csv"
        # shutil.copy(source, destination)
        # os.rmdir(OUT_PATH + table_id)
        return {table_id: "Successful"}
    except Exception as err:
        return {table_id: err}


if __name__ == "__main__":
    table_list = open('../statcan_url_list.txt', 'r').readlines()
    input_files = list(map(lambda x: IN_PATH + x.split(':')[1].strip() + ".csv", table_list))
    results = list(map(clean_csv, input_files))
    print(results)
