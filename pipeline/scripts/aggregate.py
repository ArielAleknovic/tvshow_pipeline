from utils import get_trino_connection,create_gold_tvshows
import pandas as pd
from io import StringIO

def aggregate_data():
    create_gold_tvshows()

    conn = get_trino_connection(schema='silver', user='pipeline')
    cur = conn.cursor()

    print("Inserindo na gold...")

    
    insert_sql = """
    INSERT INTO minio.gold.tvshows_gold
    SELECT
        status,
        language,
        COUNT(name) AS total_shows,
        CAST(AVG(runtime) AS INTEGER) AS avg_runtime,
        MIN(premiered) AS earliest_premiered,
        MAX(premiered) AS latest_premiered
    FROM minio.silver.tvshows_silver
    GROUP BY status, language
    """

    cur.execute(insert_sql)
    print("Dados agregados inseridos na camada gold com sucesso.")


aggregate_data()