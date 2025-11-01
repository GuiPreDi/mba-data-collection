#!/usr/bin/env python3
"""
Pipeline Silver (silver/dbloja):
- Baixa Parquets da Bronze via MinIO SDK
- Lê localmente com Spark (file://)
- Converte tipos (Long→Timestamp, Double→Decimal)
- Salva local e faz upload para a Silver com nome padronizado
"""

import os
import tempfile
import shutil
import glob
from typing import Iterable
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from minio import Minio
from minio.error import S3Error
from minio.deleteobjects import DeleteObject
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType, TimestampType, DecimalType,
    BooleanType, LongType,
)

# Configurações de ambiente / defaults
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

BUCKET = os.getenv("DL_BUCKET", "datalake")
BRONZE_BASE_PREFIX = "bronze/dbloja/"
SILVER_BASE_PREFIX = "silver/dbloja/"

# ----------------- Helpers MinIO -----------------

def connect_minio() -> Minio:
    """Cria cliente MinIO e valida conexão básica."""
    print("Conectando ao MinIO…")
    cli = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )
    cli.list_buckets()
    print("OK: Conectado ao MinIO.")
    return cli


def download_prefix_to_dir(minio_client: Minio, prefix: str, local_dir: str) -> int:
    """Baixa todos os .parquet de um prefixo para diretório local, preservando subpastas."""
    print(f"  Baixando arquivos de 's3://{BUCKET}/{prefix}'...")
    num_files = 0
    try:
        for obj in minio_client.list_objects(BUCKET, prefix=prefix, recursive=True):
            if obj.object_name.endswith(".parquet"):
                rel = os.path.relpath(obj.object_name, prefix)
                dst = os.path.join(local_dir, rel)
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                minio_client.fget_object(BUCKET, obj.object_name, dst)
                num_files += 1
        if num_files == 0:
            print(f"  Nenhum .parquet em {prefix}. Pulando.")
        else:
            print(f"  {num_files} arquivos baixados para {local_dir}")
        return num_files
    except Exception as e:
        print(f"  ERRO ao baixar da Bronze: {e}. Pulando tabela.")
        return 0


def remove_prefix(minio_client: Minio, prefix: str) -> None:
    """Remove todos os objetos do prefixo (overwrite simples do diretório Silver)."""
    print(f"  Limpando diretório Silver antigo: s3://{BUCKET}/{prefix}")
    try:
        to_delete: Iterable[DeleteObject] = (
            DeleteObject(o.object_name)
            for o in minio_client.list_objects(BUCKET, prefix=prefix, recursive=True)
        )
        errors = list(minio_client.remove_objects(BUCKET, to_delete))
        if errors:
            print("  Falhas ao remover objetos antigos:")
            for e in errors:
                print(f"  - {e}")
        print("  Diretório Silver limpo.")
    except Exception as e:
        print(f"  Aviso: Não foi possível limpar diretório Silver (pode não existir): {e}")


# ----------------- Execução principal -----------------

def main():
    """Orquestra Spark + MinIO, processa tabelas e publica na Silver."""
    print("Iniciando clientes Spark e MinIO...")
    spark = None
    workdir = tempfile.mkdtemp(prefix="spark_silver_dbloja_")
    print(f"Usando diretório temporário: {workdir}")

    try:
        # Spark local para leitura/escrita de Parquet
        spark = (
            SparkSession.builder
            .appName("ProcessamentoSilver_dbloja")
            .master("local[*]")
            .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        print("Sessão Spark iniciada.")

        # Cliente MinIO
        minio = connect_minio()

        # Schemas esperados da Bronze
        schema_cliente = StructType(
            [
                StructField("id", LongType(), True),
                StructField("nome", StringType(), True),
                StructField("email", StringType(), True),
                StructField("telefone", StringType(), True),
                StructField("data_cadastro", LongType(), True),
                StructField("is_date", BooleanType(), True),
            ]
        )
        schema_categorias_produto = StructType(
            [
                StructField("id", LongType(), True),
                StructField("nome", StringType(), True),
                StructField("descricao", StringType(), True),
            ]
        )
        schema_pedido_cabecalho = StructType(
            [
                StructField("id", LongType(), True),
                StructField("id_cliente", LongType(), True),
                StructField("data_pedido", LongType(), True),
                StructField("valor_total", DoubleType(), True),
            ]
        )
        schema_pedido_itens = StructType(
            [
                StructField("id", LongType(), True),
                StructField("id_pedido", LongType(), True),
                StructField("id_produto", LongType(), True),
                StructField("quantidade", LongType(), True),
                StructField("preco_unitario", DoubleType(), True),
            ]
        )
        schema_produto = StructType(
            [
                StructField("id", LongType(), True),
                StructField("nome", StringType(), True),
                StructField("id_categoria", LongType(), True),
                StructField("preco", DoubleType(), True),
                StructField("data_atualizacao", LongType(), True),
            ]
        )

        schemas = {
            "cliente": schema_cliente,
            "categorias_produto": schema_categorias_produto,
            "pedido_cabecalho": schema_pedido_cabecalho,
            "pedido_itens": schema_pedido_itens,
            "produto": schema_produto,
        }

        # Colunas de época (epoch micros) que devem virar Timestamp
        colunas_timestamp = {
            "cliente": ["data_cadastro"],
            "pedido_cabecalho": ["data_pedido"],
            "produto": ["data_atualizacao"],
        }

        # Colunas monetárias que devem virar Decimal(10,2)
        colunas_decimal = {
            "pedido_cabecalho": {"valor_total": DecimalType(10, 2)},
            "pedido_itens": {"preco_unitario": DecimalType(10, 2)},
            "produto": {"preco": DecimalType(10, 2)},
        }

        tabelas = list(schemas.keys())
        print(f"\n--- Carga Full (overwrite) para: {tabelas} ---")

        # Loop de processamento por tabela
        for nome_tabela in tabelas:
            print(f"\nProcessando: {nome_tabela}")

            # Prefixos no MinIO e diretórios locais
            bronze_prefix = f"{BRONZE_BASE_PREFIX}{nome_tabela}/"
            silver_prefix = f"{SILVER_BASE_PREFIX}{nome_tabela}/"
            local_bronze = os.path.join(workdir, "bronze", nome_tabela)
            local_silver = os.path.join(workdir, "silver", nome_tabela)

            # Download da Bronze
            if download_prefix_to_dir(minio, bronze_prefix, local_bronze) == 0:
                continue

            # Leitura com schema e conversões de tipo
            print(f"  Lendo: file://{local_bronze}")
            schema = schemas[nome_tabela]
            df = spark.read.schema(schema).parquet(f"file://{local_bronze}")

            # Long → Timestamp
            if nome_tabela in colunas_timestamp:
                for c in colunas_timestamp[nome_tabela]:
                    df = df.withColumn(c, F.timestamp_micros(F.col(c)))

            # Double → Decimal
            if nome_tabela in colunas_decimal:
                for c, t in colunas_decimal[nome_tabela].items():
                    df = df.withColumn(c, F.col(c).cast(t))

            # Reduz para 1 arquivo
            df = df.coalesce(1)

            # Escrita local em Parquet
            print(f"  Salvando em: file://{local_silver}")
            df.printSchema()
            df.write.mode("overwrite").parquet(f"file://{local_silver}")

            # Descobre o part-*.parquet gerado
            parts = glob.glob(f"{local_silver}/part-*.parquet")
            if not parts:
                print("  ERRO: Spark não produziu part-*.parquet. Pulando upload.")
                continue
            part_local = parts[0]

            # Nome final alinhado com Bronze: nome_tabela_YYYYMMDD_HHMMSS.parquet
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            final_name = f"{nome_tabela}_{ts}.parquet"
            silver_obj = f"{silver_prefix}{final_name}"

            # Overwrite simples no Silver
            remove_prefix(minio, silver_prefix)

            # Upload do arquivo final
            print(f"  Upload: '{part_local}' → 's3://{BUCKET}/{silver_obj}'")
            minio.fput_object(
                BUCKET,
                silver_obj,
                part_local,
                content_type="application/octet-stream",
            )
            print(f"  OK: s3://{BUCKET}/{silver_prefix}")

        print("\n--- Processamento Full concluído ---")

    except Exception:
        print("\n--- ERRO DURANTE O PROCESSAMENTO ---")
        import traceback
        traceback.print_exc()

    finally:
        # Libera recursos (Spark e diretórios temporários)
        if spark:
            spark.stop()
            print("\nSessão Spark encerrada.")
        try:
            shutil.rmtree(workdir)
            print(f"Diretório temporário {workdir} removido.")
        except Exception as e:
            print(f"AVISO: Não foi possível remover {workdir}. Erro: {e}")


if __name__ == "__main__":
    main()
