from trino.dbapi import connect
import pymysql
from utils import get_pending_files, create_silver_tvshows, mark_as_loaded


def transform_data():
    pending_files = get_pending_files(bucket="bronze", stage="silver")
    if not pending_files:
        print("Nenhum arquivo pendente para processar.")
        return

    # Garante tabela silver criada antes da transformação
    create_silver_tvshows()

    conn = connect(
        host='localhost',
        port=8080,
        user='pipeline',
        catalog='minio',
        schema='bronze'
    )
    cur = conn.cursor()

    for object_key in pending_files:
        filter_path = f"%{object_key}%"

        print(f"Processando arquivo: {object_key}")

        insert_sql = f"""
        INSERT INTO minio.silver.tvshows_silver
        SELECT
            name,
            type,
            language,
            regexp_replace(CAST(genres AS VARCHAR), '\\[|\\]|''', '') AS genres,
            status,
            TRY_CAST(runtime AS INTEGER) AS runtime,
            TRY_CAST(premiered AS DATE) AS premiered,
            officialSite
        FROM minio.bronze.tvshows
        WHERE "$path" LIKE '{filter_path}'
        """
        cur.execute(insert_sql)

        print(f"Arquivo {object_key} transformado e inserido na silver.")

        mark_as_loaded(object_key, stage="silver")

    print("Transformação concluída para todos arquivos pendentes.")

transform_data()
