from minio import Minio
import os
from glob import glob
from datetime import datetime

minio_client = Minio(
            "minio:9000",
            access_key="minioadmin", 
            secret_key="minioadmin", 
            secure=False
        )
        
        # Ping básico (list_buckets) para verificar a conexão

minio_client.list_buckets()
print("Conexão com o Minio estabelecida com sucesso!")

folder_date = datetime.now().strftime("%Y%m%d")
file_date = datetime.now().strftime("%Y%m%d_%H%M%S")

bucket_name = "datalake"

full_path = f"bronze/json"

json_dir = "/workspace/json"  # caminho local onde estão os .json

# Garante que o bucket existe
if not minio_client.bucket_exists(bucket_name):
    print(f"Bucket '{bucket_name}' não existe.")
else:
    print(f"Bucket '{bucket_name}' encontrado!")

# Faz upload de todos os .json
json_files = glob(os.path.join(json_dir, "*.json"))

if not json_files:
    print(f"Nenhum arquivo JSON encontrado em {json_dir}")
else:
    for file_path in json_files:
        folder_name = os.path.basename(file_path).replace('.json','')
        file_name = folder_name + '_' + file_date + '.json'
        object_name = f"{full_path}/{folder_name}/data={folder_date}/{file_name}"
        file_size = os.path.getsize(file_path)

        with open(file_path, "rb") as file_data:
            minio_client.put_object(
                bucket_name,
                object_name,
                file_data,
                length=file_size,
                content_type="application/json"
            )
        print(f"Enviado: {file_name} → {bucket_name}/{object_name}")