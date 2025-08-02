from utils import save_to_minio,insert_metadata,create_metadata_table,create_bronze_tvshows
from io import StringIO
from datetime import datetime
import pymysql
import requests
import pandas as pd


def extract_exchange_data():
    create_metadata_table()
    create_bronze_tvshows()
    all_results = []

    for i in range(10):
        response = requests.get("http://api.tvmaze.com/search/shows?q=postman")
        if response.status_code == 200:
            data = response.json()
            for item in data:
                show = item.get("show", {})
                all_results.append({
                    "name": show.get("name"),
                    "type": show.get("type"),
                    "language": show.get("language"),
                    "genres": show.get("genres"),
                    "status": show.get("status"),
                    "runtime": show.get("runtime"),
                    "premiered": show.get("premiered"),
                    "officialSite": show.get("officialSite"),
                })
        else:
            print(f"Erro na requisição {i+1}: {response.status_code}")

    df = pd.DataFrame(all_results)
    csv_data = StringIO()
    df.to_csv(csv_data, index=False)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    key = f'tvshows/daily_{timestamp}.csv'
    save_to_minio('bronze', key, csv_data.getvalue())

    # Inserir metadados no MariaDB
    insert_metadata('bronze', key)



extract_exchange_data()
