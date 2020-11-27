import requests
import os
import sys
import zipfile

from multiprocessing import Pool, cpu_count
from stats_can import scwds

OUT_PATH = "../data/raw/statcan/"
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
        with requests.get(url_path, stream=True) as response, open(download_file, 'wb') as out_file:
            response.raise_for_status()
            print(f"Downloading - {title}")
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    out_file.write(chunk)
            print(f"Download complete - {title}")
    input_zip = zipfile.ZipFile(download_file)
    input_file = input_zip.namelist()[0]
    if os.path.exists(OUT_PATH + input_file):
        print(f"Extraction skipped - {title} ")
    else:
        input_zip.extract(input_file, OUT_PATH)
        print(f"Extracted {input_file} for - {title} ")
    input_zip.close()
    return {table_id: title}


if __name__ == "__main__":
    print("There are {} CPUs on this machine ".format(cpu_count()))
    pool = Pool(cpu_count())
    table_list = open('../statcan_url_list.txt', 'r').readlines()
    results = pool.map(download_zips, table_list)
    pool.close()
    pool.join()
    print("Stat_can files used in the project are : ", results)
