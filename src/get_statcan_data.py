import requests
import os
from stats_can import scwds

print(os.getcwd())
os.chdir("..")      # Shift back to project repo
OUTPATH = "./data/raw/statcan/"
os.makedirs(OUTPATH, exist_ok=True)

table_list = open('statcan_url_list.txt', 'r')
for line in table_list:
    title = line.split(':')[0].strip()
    table = line.split(':')[1].strip()
    url_path = scwds.get_full_table_download(table)
    _, ext = os.path.splitext(url_path)
    file_name = os.path.join(OUTPATH, title + ext)
    with requests.get(url_path, stream=True) as res, open(file_name, 'wb') as out_file:
        res.raise_for_status()
        print(f"Downloading - {title}")
        for chunk in res.iter_content(chunk_size=8192):
            if chunk:
                out_file.write(chunk)
        print(f"Download complete - {title} ")
