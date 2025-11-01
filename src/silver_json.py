import os
import re
import shutil
import tempfile
from typing import Iterable, Tuple

from minio import Minio
from minio.deleteobjects import DeleteObject
from pyspark.sql import SparkSession, functions as F, types as T

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE     = os.getenv("MINIO_SECURE", "false").lower() == "true"

BUCKET = os.getenv("DL_BUCKET", "datalake")

# Pastas BRONZE
BRONZE_ROOT = "bronze/json/"
FAM_EXTRATO   = "dados_extrato/"
FAM_PEDIDOS   = "dados_pedidos/"
FAM_PRODUTOS  = "dados_produtos/"
FAM_TAGS      = "dados_tags/"

# Destinos SILVER
SILVER_TRANSACOES      = "silver/json/transacoes/"
SILVER_PEDIDOS_HDR     = "silver/json/pedidos_externos/"
SILVER_PEDIDOS_ITENS   = "silver/json/pedidos_externos_itens/"
SILVER_PRODUTOS        = "silver/json/produtos_parceiros/"
SILVER_TAGS            = "silver/json/tags_produtos/"

def connect_minio() -> Minio:
    cli = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )
    cli.list_buckets()
    return cli

def latest_partition_and_file(cli: Minio, family_prefix: str) -> Tuple[str, str]:
    base = f"{BRONZE_ROOT}{family_prefix}"
    re_data = re.compile(rf"^{re.escape(base)}data=(\d{{8}})/$")
    dates = set()
    for obj in cli.list_objects(BUCKET, prefix=base, recursive=False):
        m = re_data.match(obj.object_name)
        if m:
            dates.add(m.group(1))
    if not dates:
        raise RuntimeError(f"Nenhuma partição data=YYYYMMDD em s3://{BUCKET}/{base}")

    max_date = max(dates)
    date_prefix = f"{base}data={max_date}/"

    latest = None
    for obj in cli.list_objects(BUCKET, prefix=date_prefix, recursive=False):
        if obj.object_name.lower().endswith(".json"):
            if latest is None or obj.last_modified > latest.last_modified:
                latest = obj
    if latest is None:
        raise RuntimeError(f"Nenhum .json em s3://{BUCKET}/{date_prefix}")

    return max_date, latest.object_name

def fget_to_local(cli: Minio, object_name: str, local_path: str):
    cli.fget_object(BUCKET, object_name, local_path)

def remove_prefix(cli: Minio, prefix: str):
    to_del: Iterable[DeleteObject] = (
        DeleteObject(o.object_name)
        for o in cli.list_objects(BUCKET, prefix=prefix, recursive=True)
    )
    errs = list(cli.remove_objects(BUCKET, to_del))
    if errs:
        raise RuntimeError(f"Falhas ao limpar {prefix}: {errs}")

def upload_directory(cli: Minio, local_dir: str, dest_prefix: str):
    for root, _, files in os.walk(local_dir):
        for f in files:
            full = os.path.join(root, f)
            rel = os.path.relpath(full, start=local_dir).replace("\\", "/")
            key = f"{dest_prefix}{rel}"
            cli.fput_object(BUCKET, key, full)
            print(f"PUT s3://{BUCKET}/{key}")

def write_parquet_local(df, out_dir: str):
    if os.path.exists(out_dir):
        shutil.rmtree(out_dir, ignore_errors=True)
    df.write.mode("overwrite").parquet(out_dir)
    for marker in ["_SUCCESS", "._SUCCESS"]:
        mpath = os.path.join(out_dir, marker)
        if os.path.exists(mpath):
            os.remove(mpath)

# ==========================================================
# 1) Extrato -> transacoes silver
# ==========================================================
def process_extrato(spark, local_json_in: str, local_out_dir: str):
    df = spark.read.option("multiline", "true").json(f"file://{local_json_in}")

    df_trn = (
        df
        .withColumn("transacao", F.explode_outer("transacoes"))
        .select(
            F.col("id_extrato"),
            F.col("cliente.cliente_id").alias("cliente_id"),
            F.col("conta.numero_conta").alias("numero_conta"),
            F.col("transacao.id_transacao").alias("id_transacao"),
            F.col("transacao.data").alias("data_transacao"),
            F.col("transacao.descricao").alias("descricao"),
            F.col("transacao.valor").alias("valor"),
            F.col("transacao.tipo").alias("tipo")
        )
    )

    write_parquet_local(df_trn, local_out_dir)

# ==========================================================
# 2) Pedidos -> header e itens silver
# ==========================================================
def process_pedidos(spark, local_json_in: str, local_out_hdr: str, local_out_itens: str):
    df = spark.read.option("multiline", "true").json(f"file://{local_json_in}")

    df_hdr = df.select(
        "id_pedido", "data_pedido", "status", "total_pedido",
        F.col("cliente.id_cliente").alias("cliente_id"),
        F.col("cliente.nome").alias("cliente_nome"),
        F.col("cliente.email").alias("cliente_email"),
        F.col("entrega.metodo").alias("entrega_metodo"),
        F.col("entrega.taxa_frete").alias("taxa_frete"),
        F.col("entrega.endereco.rua").alias("end_rua"),
        F.col("entrega.endereco.numero").alias("end_numero"),
        F.col("entrega.endereco.complemento").alias("end_complemento"),
        F.col("entrega.endereco.cidade").alias("end_cidade"),
        F.col("entrega.endereco.estado").alias("end_estado"),
        F.col("entrega.endereco.cep").alias("end_cep")
    )

    write_parquet_local(df_hdr, local_out_hdr)

    df_itens = (
        df
        .withColumn("item", F.explode_outer("itens"))
        .select(
            "id_pedido",
            F.col("item.sku").alias("sku"),
            F.col("item.produto").alias("produto"),
            F.col("item.quantidade").alias("quantidade"),
            F.col("item.preco_unitario").alias("preco_unitario")
        )
    )

    write_parquet_local(df_itens, local_out_itens)

# ==========================================================
# 3) Produtos -> flatten silver
# ==========================================================
def process_produtos(spark, local_json_in: str, local_out_dir: str):
    df_raw = spark.read.option("multiline", "true").json(f"file://{local_json_in}")
    df = df_raw.withColumn("p", F.explode_outer("produtos")).select("p.*")

    df_flat = (
        df
        .withColumn("processador", F.col("especificacoes.processador"))
        .withColumn("ram", F.col("especificacoes.ram"))
        .withColumn("armazenamento", F.col("especificacoes.armazenamento"))
        .withColumn("origem", F.col("detalhes.origem"))
        .withColumn("peso_g", F.col("detalhes.peso_g"))
        .withColumn("torra", F.col("detalhes.torra"))
        .drop("especificacoes", "detalhes", "avaliacoes")
    )

    write_parquet_local(df_flat, local_out_dir)

# ==========================================================
# 4) Tags -> explode silver
# ==========================================================
def process_tags(spark, local_json_in: str, local_out_dir: str):
    df = spark.read.option("multiline","true").json(f"file://{local_json_in}")

    empty_arr = F.array()

    df_norm = (
        df
        .withColumn("tags", F.when(F.col("tags").isNull(), empty_arr).otherwise(F.col("tags")))
        .withColumn("tag", F.explode_outer("tags"))
        .select(
            F.col("produto_id"),
            F.col("nome").alias("produto_nome"),
            F.col("tag")
        )
    )

    write_parquet_local(df_norm, local_out_dir)

# ==========================================================
def main():
    spark = SparkSession.builder.appName("Bronze->Silver Loja (MinIO SDK)").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    cli = connect_minio()

    work = tempfile.mkdtemp(prefix="silver_loja_")
    try:
        # EXTRATO
        date_extrato, obj_extrato = latest_partition_and_file(cli, FAM_EXTRATO)
        local_extrato = os.path.join(work, "dados_extrato.json")
        fget_to_local(cli, obj_extrato, local_extrato)

        out_extrato_local = os.path.join(work, "silver_transacoes")
        process_extrato(spark, local_extrato, out_extrato_local)
        remove_prefix(cli, SILVER_TRANSACOES)
        upload_directory(cli, out_extrato_local, SILVER_TRANSACOES)

        # PEDIDOS
        date_ped, obj_ped = latest_partition_and_file(cli, FAM_PEDIDOS)
        local_ped = os.path.join(work, "dados_pedidos.json")
        fget_to_local(cli, obj_ped, local_ped)

        out_ped_hdr   = os.path.join(work, "silver_pedidos_hdr")
        out_ped_itens = os.path.join(work, "silver_pedidos_itens")
        process_pedidos(spark, local_ped, out_ped_hdr, out_ped_itens)

        remove_prefix(cli, SILVER_PEDIDOS_HDR)
        upload_directory(cli, out_ped_hdr, SILVER_PEDIDOS_HDR)

        remove_prefix(cli, SILVER_PEDIDOS_ITENS)
        upload_directory(cli, out_ped_itens, SILVER_PEDIDOS_ITENS)

        # PRODUTOS
        date_prod, obj_prod = latest_partition_and_file(cli, FAM_PRODUTOS)
        local_prod = os.path.join(work, "dados_produtos.json")
        fget_to_local(cli, obj_prod, local_prod)

        out_prod = os.path.join(work, "silver_produtos")
        process_produtos(spark, local_prod, out_prod)

        remove_prefix(cli, SILVER_PRODUTOS)
        upload_directory(cli, out_prod, SILVER_PRODUTOS)

        # TAGS
        date_tags, obj_tags = latest_partition_and_file(cli, FAM_TAGS)
        local_tags = os.path.join(work, "dados_tags.json")
        fget_to_local(cli, obj_tags, local_tags)

        out_tags = os.path.join(work, "silver_tags")
        process_tags(spark, local_tags, out_tags)

        remove_prefix(cli, SILVER_TAGS)
        upload_directory(cli, out_tags, SILVER_TAGS)

        print("\n Silver publicado com sucesso.")
        print(f" - transacoes  => s3://{BUCKET}/{SILVER_TRANSACOES}")
        print(f" - pedidos_hdr => s3://{BUCKET}/{SILVER_PEDIDOS_HDR}")
        print(f" - pedidos_itens => s3://{BUCKET}/{SILVER_PEDIDOS_ITENS}")
        print(f" - produtos => s3://{BUCKET}/{SILVER_PRODUTOS}")
        print(f" - tags => s3://{BUCKET}/{SILVER_TAGS}")

    finally:
        shutil.rmtree(work, ignore_errors=True)

if __name__ == "__main__":
    main()
