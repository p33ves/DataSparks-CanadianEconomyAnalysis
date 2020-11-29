import os
import sys

from pyspark.sql import SparkSession, functions, types

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
spark = SparkSession.builder.appName('statcan data cleanse').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

IN_PATH = "../data/raw/statcan/"
OUT_PATH = "../schema/statcan/"
os.makedirs(OUT_PATH, exist_ok=True)


def get_schema(file_name):
    table_id = os.path.basename(file_name).split('.')[0]
    try:
        input_data = spark.read.option("header", "true") \
            .csv(file_name, inferSchema=True)
        with open(OUT_PATH + table_id + ".json", 'w') as out_file:
            out_file.write(input_data.schema.json())
        return {table_id: "Successful"}
    except Exception as err:
        return {table_id: err}


if __name__ == "__main__":
    table_list = open('../statcan_url_list.txt', 'r').readlines()
    input_files = list(map(lambda x: IN_PATH + x.split(':')[1].strip() + ".csv", table_list))
    results = list(map(get_schema, input_files))
    print(results)
