import psycopg2
from minio import Minio
from io import BytesIO
from minio.error import S3Error
import sys
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
import json

JSON_CONTROL_FILE = "bronze/dbloja/data_atualizacao.json"

def setup_minio(minio_client, bucket_name, folder_path):
    """Verifica e cria bucket e pastas base no MinIO."""
    try:
        found = minio_client.bucket_exists(bucket_name)
        if not found:
            minio_client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' criado.")
        else:
            print(f"Bucket '{bucket_name}' já existe.")

        for folder in folder_path:
            try:
                minio_client.stat_object(bucket_name, folder)
            except S3Error as e:
                if e.code == 'NoSuchKey':
                    minio_client.put_object(
                        bucket_name=bucket_name,
                        object_name=folder,
                        data=BytesIO(b""),
                        length=0
                    )
                    print(f"Pasta '{folder}' criada.")
        print("Setup do MinIO concluído.")
    except Exception as e:
        print(f"Erro durante setup do MinIO: {e}")
        sys.exit(1)

def get_last_run_timestamp(minio_client, bucket_name):
    """Lê o timestamp da última execução do arquivo JSON no MinIO."""
    try:
        response = minio_client.get_object(bucket_name, JSON_CONTROL_FILE)
        data = response.read()
        response.close()
        response.release_conn()

        control_data = json.loads(data)
        return control_data.get('last_run')
    except S3Error as e:
        if e.code == 'NoSuchKey':
            print(f"Arquivo '{JSON_CONTROL_FILE}' não encontrado. Carga total.")
            return None
    except Exception as e:
        print(f"Erro ao ler JSON de controle: {e}")
        return None

def update_last_run_timestamp(minio_client, bucket_name, current_timestamp_iso):
    """Atualiza o JSON de controle com o timestamp atual no MinIO."""
    try:
        control_data = {'last_run': current_timestamp_iso}
        json_bytes = json.dumps(control_data, indent=2).encode('utf-8')
        json_buffer = BytesIO(json_bytes)

        minio_client.put_object(
            bucket_name,
            JSON_CONTROL_FILE,
            data=json_buffer,
            length=len(json_bytes),
            content_type='application/json'
        )
        print(f"Arquivo de controle atualizado: {current_timestamp_iso}")
    except Exception as e:
        print(f"Erro ao salvar timestamp: {e}")

def extract_postgres_to_minio_partitioned(engine, minio_client, bucket_name, bronze_base_path, db_name_path, db_schema):
    """Extrai tabelas do Postgres e salva no MinIO com particionamento e lógica incremental."""
    print(f"Iniciando ingestão particionada do schema '{db_schema}'...")

    current_run_iso = datetime.now().isoformat()
    last_run_iso = get_last_run_timestamp(minio_client, bucket_name)
    NOME_COLUNA_INCREMENTAL = "data_atualizacao"

    try:
        with engine.connect() as conn:
            query_lista_tabelas = f"""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = '{db_schema}' AND table_type = 'BASE TABLE';
            """
            resultado = conn.execute(text(query_lista_tabelas))
            tabelas = [row[0] for row in resultado]

            if not tabelas:
                print(f"Nenhuma tabela encontrada no schema '{db_schema}'.")
                return

            for nome_tabela in tabelas:
                print(f"Processando tabela: {nome_tabela}...")

                # Define consulta conforme incremental ou carga total
                if nome_tabela == 'produto' and last_run_iso:
                    query_tabela_str = f"""
                        SELECT * FROM {db_schema}.{nome_tabela}
                        WHERE {NOME_COLUNA_INCREMENTAL} > '{last_run_iso}'
                          AND {NOME_COLUNA_INCREMENTAL} <= '{current_run_iso}'
                    """
                else:
                    query_tabela_str = f"SELECT * FROM {db_schema}.{nome_tabela}"

                df = pd.read_sql(text(query_tabela_str), conn)

                if df.empty:
                    print(f"Tabela '{nome_tabela}' vazia ou sem novos dados.")
                    continue

                # Define nome e caminho do arquivo parquet
                now_parquet = datetime.now()
                date_partition_str = now_parquet.strftime("data=%Y%m%d")
                file_timestamp_str = now_parquet.strftime("%Y%m%d_%H%M%S")
                file_name = f"{nome_tabela}_{file_timestamp_str}.parquet"
                object_name = f"{bronze_base_path}{nome_tabela}/{date_partition_str}/{file_name}"

                parquet_buffer = BytesIO()
                df.to_parquet(parquet_buffer, index=False, compression='snappy', engine='pyarrow')
                data_length = parquet_buffer.tell()
                parquet_buffer.seek(0)

                minio_client.put_object(
                    bucket_name=bucket_name,
                    object_name=object_name,
                    data=parquet_buffer,
                    length=data_length,
                    content_type='application/octet-stream'
                )
                print(f"Tabela salva em: '{object_name}' ({len(df)} linhas)")

        update_last_run_timestamp(minio_client, bucket_name, current_run_iso)
        print("Ingestão concluída.")

    except Exception as e:
        print(f"Erro durante ingestão: {e}")

def main():
    """Executa setup, conexão e ETL completo."""

    MINIO_ENDPOINT = "minio:9000"
    MINIO_ACCESS_KEY = "minioadmin"
    MINIO_SECRET_KEY = "minioadmin"

    BUCKET_NAME = "datalake"
    FOLDERS_TO_CREATE = [
        "bronze/dbloja/"
    ]

    POSTGRES_HOST = "db"
    POSTGRES_DB = "mydb"
    POSTGRES_USER = "myuser"
    POSTGRES_PASS = "mypassword"
    POSTGRES_PORT = "5432"
    POSTGRES_SCHEMA = "db_loja"

    DB_NAME_FOR_PATH = "dbloja"
    BRONZE_DEST_PATH = f"bronze/{DB_NAME_FOR_PATH}/"

    try:
        minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )
        minio_client.list_buckets()
        setup_minio(minio_client, BUCKET_NAME, FOLDERS_TO_CREATE)
    except Exception as e:
        print(f"Erro ao configurar MinIO: {e}")
        return

    try:
        db_string = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASS}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        engine = create_engine(db_string)

        with engine.connect() as conn:
            print("Conexão com PostgreSQL bem-sucedida.")

        extract_postgres_to_minio_partitioned(
            engine=engine,
            minio_client=minio_client,
            bucket_name=BUCKET_NAME,
            bronze_base_path=BRONZE_DEST_PATH,
            db_name_path=DB_NAME_FOR_PATH,
            db_schema=POSTGRES_SCHEMA
        )
    except Exception as e:
        print(f"Erro ao conectar ou executar ETL no PostgreSQL: {e}")
    finally:
        print("Script finalizado.")

if __name__ == '__main__':
    main()
