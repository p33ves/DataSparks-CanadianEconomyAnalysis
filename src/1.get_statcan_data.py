import requests
import os
import sys
import zipfile

from stats_can import scwds
from pyspark.sql import SparkSession

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
spark = SparkSession.builder.appName('statcan data download').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

OUT_PATH = "s3://mysparks/data/raw/statcan/"
os.makedirs(OUT_PATH, exist_ok=True)


def download_zips(line):
    title = line.split(':')[0].strip()
    table_id = line.split(':')[1].strip()
    url_path = scwds.get_full_table_download(table_id)
    _, ext = os.path.splitext(url_path)
    download_file = os.path.join(OUT_PATH, title + ext)
    if os.path.exists(download_file):
        print(f"Download skipped - {title} ")
    else:
        try:
            with requests.get(url_path, stream=True) as response, open(download_file, 'wb') as out_file:
                response.raise_for_status()
                print(f"Downloading - {title}")
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        out_file.write(chunk)
                print(f"Download complete - {title}")
        except requests.exceptions.RequestException as err:
            return {table_id: err}
    try:
        input_zip = zipfile.ZipFile(download_file)
        input_file = input_zip.namelist()[0]
        if os.path.exists(OUT_PATH + input_file):
            print(f"Extraction skipped - {title} ")
        else:
            input_zip.extract(input_file, OUT_PATH)
            print(f"Extracted {input_file} for - {title} ")
        input_zip.close()
        os.remove(download_file)
        return {table_id: "Successful"}
    except Exception as err:
        return {table_id: err}


if __name__ == "__main__":
    table_list = sc.parallelize(open('s3://mysparks/statcan_url_list.txt', 'r').readlines())
    results = table_list.map(download_zips).collect()
    print("stat_can files download results are: ", results)
