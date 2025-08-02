import boto3
import pymysql
from trino.dbapi import connect
from trino.exceptions import TrinoUserError

# --------------------- MINIO ---------------------

def get_minio_client():
    return boto3.client(
        's3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='minio_access_key',
        aws_secret_access_key='minio_secret_key'
    )

def save_to_minio(bucket, key, content):
    client = get_minio_client()
    client.put_object(Bucket=bucket, Key=key, Body=content)

def read_from_minio(bucket, key):
    client = get_minio_client()
    obj = client.get_object(Bucket=bucket, Key=key)
    return obj['Body'].read().decode('utf-8')

def list_csv_files(bucket, prefix):
    client = get_minio_client()
    response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if 'Contents' not in response:
        return []
    return [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.csv')]

def list_all_objects(bucket, prefix):
    client = get_minio_client()
    paginator = client.get_paginator('list_objects_v2')
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys

# --------------------- MariaDB (Metadata) ---------------------

def get_mysql_connection():
    return pymysql.connect(
        host='localhost',
        user='admin',
        password='admin',
        database='metastore_db',
        port=3306
    )

def create_metadata_table():
    connection = get_mysql_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS metadata_files (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    bucket_name VARCHAR(255),
                    object_key VARCHAR(1024),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    loaded_silver BOOLEAN DEFAULT FALSE,
                    loaded_gold BOOLEAN DEFAULT FALSE
                );
            """)
        connection.commit()
    finally:
        connection.close()

def insert_metadata(bucket, key):
    connection = get_mysql_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO metadata_files (bucket_name, object_key)
                VALUES (%s, %s)
            """, (bucket, key))
        connection.commit()
    finally:
        connection.close()

def get_pending_files(bucket=None, stage=None):
    connection = get_mysql_connection()
    try:
        with connection.cursor() as cursor:
            query = "SELECT object_key FROM metadata_files WHERE 1=1"
            params = []

            if stage == 'silver':
                query += " AND loaded_silver = FALSE"
            if stage == 'gold':
                query += " AND loaded_gold = FALSE"
            if bucket:
                query += " AND bucket_name = %s"
                params.append(bucket)

            cursor.execute(query, tuple(params))
            return [row[0] for row in cursor.fetchall()]
    finally:
        connection.close()

def mark_as_loaded(object_key, stage):
    assert stage in ['silver', 'gold'], "Stage must be 'silver' or 'gold'"
    column = 'loaded_silver' if stage == 'silver' else 'loaded_gold'
    bucket = 'bronze' if stage == 'silver' else 'silver'

    connection = get_mysql_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"""
                UPDATE metadata_files
                SET {column} = TRUE
                WHERE object_key = %s AND bucket_name = %s
            """, (object_key, bucket))
        connection.commit()
    finally:
        connection.close()

# --------------------- TRINO ---------------------

def get_trino_connection(schema='default', user='trino'):
    return connect(
        host='localhost',
        port=8080,
        user=user,
        catalog='minio',
        schema=schema
    )

def create_schema(schema_name, location):
    conn = get_trino_connection(schema='default', user='trino')
    cur = conn.cursor()
    try:
        cur.execute(f"""
            CREATE SCHEMA IF NOT EXISTS minio.{schema_name}
            WITH (location = '{location}')
        """)
        print(f"Schema {schema_name} criado com sucesso.")
    except TrinoUserError as e:
        print(f"Erro ao criar schema {schema_name}:", e)

def create_table(schema_name, table_name, ddl, user='pipeline'):
    conn = get_trino_connection(schema=schema_name, user=user)
    cur = conn.cursor()
    try:
        cur.execute(ddl)
        print(f"Tabela {schema_name}.{table_name} criada com sucesso.")
    except TrinoUserError as e:
        print(f"Erro ao criar tabela {table_name}:", e)

def create_bronze_tvshows():
    create_schema('bronze', 's3a://bronze/')
    create_table('bronze', 'tvshows', """
        CREATE TABLE IF NOT EXISTS minio.bronze.tvshows (
            name VARCHAR,
            type VARCHAR,
            language VARCHAR,
            genres VARCHAR,
            status VARCHAR,
            runtime VARCHAR,
            premiered VARCHAR,
            officialSite VARCHAR
        )
        WITH (
            external_location = 's3a://bronze/tvshows/',
            format = 'CSV',
            csv_separator = ',',
            skip_header_line_count = 1
        )
    """)

def create_silver_tvshows():
    create_schema('silver', 's3a://silver/')
    create_table('silver', 'tvshows_silver', """
        CREATE TABLE IF NOT EXISTS minio.silver.tvshows_silver (
            name VARCHAR,
            type VARCHAR,
            language VARCHAR,
            genres VARCHAR,
            status VARCHAR,
            runtime INTEGER,
            premiered DATE,
            officialSite VARCHAR
        )
        WITH (
            format = 'ORC',
            external_location = 's3a://silver/tvshows_silver/'
        )
    """)

def create_gold_tvshows():
    create_schema('gold', 's3a://gold/')
    create_table('gold', 'tvshows_gold', """
        CREATE TABLE IF NOT EXISTS minio.gold.tvshows_gold (
            status VARCHAR,
            language VARCHAR,
            total_shows INTEGER,
            avg_runtime DOUBLE,
            earliest_premiered DATE,
            latest_premiered DATE
        )
        WITH (
            format = 'ORC',
            external_location = 's3a://gold/tvshows_gold/'
        )
    """)
