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
    input_schema = json.load(open(SCHEMA_PATH+table_id+".json", 'r'))
    try:
        input_data = spark.read.option("header", "true") \
            .csv(file_name, schema=types.StructType.fromJson(input_schema))
        data_in_range = input_data.where(
            input_data['REF_DATE'].startswith('201') |
            input_data['REF_DATE'].startswith('202')) \
            .coalesce(1)
        data_in_range.write\
            .option("header", "true") \
            .csv(OUT_PATH+table_id)
        # os.remove(file_name)
        return {table_id: "Successful"}
    except Exception as err:
        return {table_id: err}


if __name__ == "__main__":
    table_list = open('../statcan_url_list.txt', 'r').readlines()
    input_files = list(map(lambda x: IN_PATH + x.split(':')[1].strip() + ".csv", table_list))
    results = list(map(clean_csv, input_files))
    print(results)
