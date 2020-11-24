import requests
import os
from stats_can import scwds

# print(os.getcwd())
os.chdir("..")      # Shift back to project repo

table_list = open('statcan_url_list.txt', 'r')
for line in table_list:
    title = line.split(':')[0].strip()
    table = line.split(':')[1].strip()
    url = scwds.get_full_table_download(table)
    with requests.get(url, stream=True) as res:
        res.raise_for_status()
        print(f"Downloading - {title}")
        with open(f'data/{title}.zip', 'wb') as target:
            for chunk in res.iter_content(chunk_size=8192):
                if chunk:
                    target.write(chunk)
        print(f"Download complete - {title} ")
