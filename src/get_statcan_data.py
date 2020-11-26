import requests
import os
from stats_can import scwds

OUTPATH = "../data/raw/statcan/"
os.makedirs(OUTPATH, exist_ok=True)

table_list = open('../statcan_url_list.txt', 'r')
for line in table_list:
    title = line.split(':')[0].strip()
    table_id = line.split(':')[1].strip()
    url_path = scwds.get_full_table_download(table_id)
    _, ext = os.path.splitext(url_path)
    file_name = os.path.join(OUTPATH, title + ext)
    if os.path.exists(file_name):
        print(f"Download skipped - {title} ")
        continue
    with requests.get(url_path, stream=True) as res, open(file_name, 'wb') as out_file:
        res.raise_for_status()
        print(f"Downloading - {title}")
        for chunk in res.iter_content(chunk_size=8192):
            if chunk:
                out_file.write(chunk)
        print(f"Download complete - {title} ")
