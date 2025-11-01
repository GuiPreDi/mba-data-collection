#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pipeline sem s3a://:
- Usa MinIO SDK para listar/baixar o JSON (bronze)
- Lê localmente com Spark (file://)
- Salva Parquet local
- Sobe os arquivos gerados para o MinIO (silver) usando put_object/fput_object
"""

import os
import re
import io
import tempfile
import shutil
from typing import Iterable
from minio import Minio
from minio.deleteobjects import DeleteObject
from pyspark.sql import SparkSession, functions as F

# ---------------- CONFIG ---------------- #
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE     = os.getenv("MINIO_SECURE", "false").lower() == "true"

BUCKET           = os.getenv("DL_BUCKET", "datalake")
BRONZE_PREFIX    = "bronze/api/"
SILVER_PREFIX    = "silver/ibge_uf/"  # destino no MinIO
# ---------------------------------------- #

def connect_minio() -> Minio:
    print("Conectando ao MinIO…")
    cli = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )
    # sanity check
    cli.list_buckets()
    print("OK: Conectado ao MinIO.")
    return cli

def find_latest_json(minio_client: Minio) -> str:
    """Retorna o object_name do JSON mais recente dentro da MAIOR pasta data=YYYYMMDD."""
    re_data = re.compile(r"^bronze/api/data=(\d{8})/?")
    dates = set()

    for obj in minio_client.list_objects(BUCKET, prefix=BRONZE_PREFIX, recursive=False):
        m = re_data.match(obj.object_name)
        if m:
            dates.add(m.group(1))

    if not dates:
        raise RuntimeError("Nenhuma pasta data=YYYYMMDD em bronze/api/")

    max_date = max(dates)
    print(f"Maior data: {max_date}")

    latest = None
    for obj in minio_client.list_objects(BUCKET, prefix=f"{BRONZE_PREFIX}data={max_date}/", recursive=False):
        if obj.object_name.lower().endswith(".json"):
            if latest is None or obj.last_modified > latest.last_modified:
                latest = obj

    if latest is None:
        raise RuntimeError(f"Nenhum .json em {BRONZE_PREFIX}data={max_date}/")

    print(f"Último JSON: {latest.object_name} (last_modified={latest.last_modified})")
    return latest.object_name  # ex: bronze/api/data=20251025/ibge-uf_20251025_175612.json

def download_object_to_file(minio_client: Minio, object_name: str, local_path: str):
    """Baixa um objeto do MinIO para um arquivo local."""
    print(f"Baixando s3://{BUCKET}/{object_name} -> {local_path}")
    # stream para disco (evita estourar memória)
    minio_client.fget_object(BUCKET, object_name, local_path)
    print("Download concluído.")

def remove_prefix(minio_client: Minio, prefix: str):
    """Apaga TODO o prefixo no bucket (overwrite manual)."""
    print(f"Removendo prefixo existente: s3://{BUCKET}/{prefix}")
    to_delete: Iterable[DeleteObject] = (
        DeleteObject(obj.object_name)
        for obj in minio_client.list_objects(BUCKET, prefix=prefix, recursive=True)
    )
    # A API remove_objects é batch e pode retornar erros por item
    errors = list(minio_client.remove_objects(BUCKET, to_delete))
    if errors:
        for e in errors:
            print("Falha ao remover:", e)
        raise RuntimeError("Falhas ao remover objetos antigos no Silver.")
    print("Prefixo limpo.")

def upload_directory(minio_client: Minio, local_dir: str, dest_prefix: str):
    """Sobe todos os arquivos de local_dir para BUCKET/dest_prefix preservando nomes."""
    print(f"Subindo {local_dir} para s3://{BUCKET}/{dest_prefix}")
    for root, _, files in os.walk(local_dir):
        for fname in files:
            full = os.path.join(root, fname)
            # chave = dest_prefix + caminho_relativo
            rel = os.path.relpath(full, start=local_dir).replace("\\", "/")
            key = f"{dest_prefix}{rel}"
            minio_client.fput_object(BUCKET, key, full)
            print(f"PUT {key}")
    print("Upload concluído.")

def main():
    print("Iniciando a sessão Spark com o dataset de produtos…")
    spark = SparkSession.builder.appName("TesteProdutosSpark").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    minio = connect_minio()
    latest_obj = find_latest_json(minio)

    # Diretórios temporários
    workdir = tempfile.mkdtemp(prefix="ibge_minio_")
    try:
        local_json = os.path.join(workdir, "bronze_ibge.json")
        download_object_to_file(minio, latest_obj, local_json)

        # Ler JSON (array) localmente
        print("Lendo JSON local com Spark…")
        df_raw = (
            spark.read
            .option("multiline", "true")
            .json(f"file://{local_json}")
        )

        # Aplicar schema/casts
        df = df_raw.select(
            F.col("id").cast("int").alias("id"),
            F.col("sigla").cast("string").alias("sigla"),
            F.col("nome").cast("string").alias("nome"),
            F.col("regiao.id").cast("int").alias("regiao_id"),
            F.col("regiao.sigla").cast("string").alias("regiao_sigla"),
            F.col("regiao.nome").cast("string").alias("regiao_nome"),
        )

        print("Schema resultante:")
        df.printSchema()
        df.show(5, truncate=False)

        # Gravar Parquet local (pasta)
        local_out = os.path.join(workdir, "silver_ibge_uf_parquet")
        print(f"Gravando Parquet local em {local_out} (overwrite)…")
        (
            df.write
            .mode("overwrite")
            .parquet(local_out)
        )

        # Apagar o _SUCCESS antes do upload
        # Remover marcadores de sucesso
        for fname in ["_SUCCESS", "._SUCCESS"]:
            success_path = os.path.join(local_out, fname)
            if os.path.exists(success_path):
                os.remove(success_path)
                print(f"Removido arquivo marcador: {fname}")



        # Overwrite manual no MinIO
        remove_prefix(minio, SILVER_PREFIX)
        upload_directory(minio, local_out, SILVER_PREFIX)

        print("\n[OK] Silver publicado em s3://{}/{}".format(BUCKET, SILVER_PREFIX))
    finally:
        # limpe os temporários (remova se quiser inspecionar)
        shutil.rmtree(workdir, ignore_errors=True)

if __name__ == "__main__":
    main()
