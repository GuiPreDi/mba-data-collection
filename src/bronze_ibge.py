import json
from datetime import datetime
import requests
from minio import Minio
from io import BytesIO

def main():
    print("Tentando se conectar ao servidor Minio...")

    try:
        minio_client = Minio(
            "minio:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False
        )
        # teste simples
        minio_client.list_buckets()
        print("Conexão com o Minio estabelecida com sucesso!")
    except Exception as e:
        print(f"\nOcorreu um erro ao conectar ao Minio: {e}")
        print("Verifique se o container 'minio' está em execução e as credenciais estão corretas.")
        return

    # 1) Busca dados na API
    try:
        resp = requests.get("https://brasilapi.com.br/api/ibge/uf/v1", timeout=15)
        resp.raise_for_status()
        data = resp.json()
        print(f"API OK. Registros recebidos: {len(data)}")
    except Exception as e:
        print(f"Erro ao consultar a API: {e}")
        return

    # 2) Monta nomes de pasta/arquivo
    data_hoje = datetime.now().strftime("%Y%m%d")
    ts_completo = datetime.now().strftime("%Y%m%d_%H%M%S")
    bucket_name = "datalake"
    object_name = f"bronze/api/data={data_hoje}/ibge-uf_{ts_completo}.json"

    # 3) Garante bucket e faz upload
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)

        payload = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
        bio = BytesIO(payload)

        minio_client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=bio,
            length=len(payload),
            content_type="application/json"
        )
        print(f"Arquivo salvo em: s3://{bucket_name}/{object_name}")
    except Exception as e:
        print(f"Erro ao salvar no Minio: {e}")

if __name__ == "__main__":
    main()
