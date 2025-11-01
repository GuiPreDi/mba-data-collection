# 🏗️ Data Lake - Arquitetura Medallion com MinIO e Spark

Este repositório implementa uma **arquitetura de Data Lake em camadas** (Medallion Architecture: Bronze → Silver), utilizando **Python, PySpark, MinIO (S3-compatible)** e **PostgreSQL** como fonte de dados.

O pipeline está dividido em etapas modulares, iniciando pela criação da infraestrutura de buckets no MinIO e seguindo com extrações, transformações e publicações em camadas refinadas.

---

## 📂 Estrutura de Diretórios

```
.
├── create_buckets.py          # Cria bucket e estrutura base no MinIO
│
├── bronze/
│   ├── bronze_dbloja.py       # Extrai dados do PostgreSQL e salva Parquet particionado
│   ├── bronze_ibge.py         # Consome API pública (IBGE) e salva JSON no MinIO
│   └── bronze_json.py         # Faz upload em lote de arquivos JSON locais
│
└── silver/
    ├── silver_dbloja.py       # Converte Parquets da Bronze (db_loja) para Silver com tipos refinados
    ├── silver_ibge.py         # Converte JSON (API IBGE) em Parquet estruturado
    └── silver_json.py         # Normaliza múltiplos JSONs de uma loja (pedidos, produtos, etc.)
```

---

## 🧱 Arquitetura Medallion

O projeto segue a arquitetura de camadas **Bronze → Silver**, onde:

| Camada | Função | Fonte / Destino |
|--------|---------|----------------|
| **Bronze** | Captura dados crus e históricos de múltiplas origens (APIs, bancos, arquivos). | PostgreSQL, APIs REST, JSON locais → MinIO (`bronze/`) |
| **Silver** | Processa, normaliza e estrutura os dados, aplicando casts, flatten e deduplicação. | MinIO (`bronze/`) → MinIO (`silver/`) via Spark |

---

## ⚙️ 1. Criação de Buckets

**Script:** `create_buckets.py`

Este script conecta ao MinIO e cria os buckets e diretórios necessários para o pipeline:

- Bucket padrão: `datalake`
- Estrutura inicial criada:
  ```
  bronze/
  silver/
  ```
  
Uso:

```bash
python create_buckets.py
```

---

## 🪣 2. Camada Bronze

### 🧩 a) PostgreSQL → Bronze (`bronze_dbloja.py`)

Extrai tabelas de um schema PostgreSQL (`db_loja`) e salva como arquivos **Parquet particionados por data** no MinIO.  
Há suporte a **cargas incrementais**, controladas por um arquivo JSON de metadados (`data_atualizacao.json`).

**Fluxo:**
1. Lê tabelas via `SQLAlchemy`.
2. Verifica o timestamp da última execução no MinIO.
3. Extrai dados novos e salva em `bronze/dbloja/<tabela>/data=YYYYMMDD/arquivo.parquet`.
4. Atualiza o JSON de controle.

---

### 🌎 b) API → Bronze (`bronze_ibge.py`)

Consome a **API pública do IBGE** via `BrasilAPI` e armazena a resposta JSON bruta no MinIO.  
Cada execução gera uma partição nova (`data=YYYYMMDD`).

```bash
python bronze_ibge.py
```

---

### 📁 c) JSON Local → Bronze (`bronze_json.py`)

Realiza o upload de todos os arquivos `.json` encontrados no diretório local `/workspace/json` para o MinIO.  
Cria partições por data automaticamente no formato:

```
bronze/json/<nome_arquivo>/data=YYYYMMDD/<nome_arquivo>_YYYYMMDD_HHMMSS.json
```

---

## 💎 3. Camada Silver

A camada **Silver** utiliza **PySpark** para ler dados da Bronze, aplicar *schema enforcement*, conversões e normalizações, e salvar versões refinadas no MinIO.

---

### 🏬 a) Silver DB Loja (`silver_dbloja.py`)

Processa os Parquets do schema `db_loja` da Bronze e gera versões estruturadas com casts e validações.

**Principais funcionalidades:**
- Leitura e inferência de schema fixo para cada tabela (`cliente`, `pedido`, `produto`, etc.).
- Conversão de colunas `LongType` para `TimestampType` e `DoubleType` para `DecimalType`.
- Escrita consolidada (1 arquivo) e upload para `silver/dbloja/<tabela>/`.

---

### 🗺️ b) Silver IBGE (`silver_ibge.py`)

Transforma o JSON mais recente do dataset IBGE em um Parquet tabular.

**Etapas:**
1. Localiza o JSON mais recente em `bronze/api/data=YYYYMMDD/`.
2. Lê via Spark (`option("multiline", "true")`).
3. Extrai e tipa colunas (`id`, `sigla`, `regiao_id`, etc.`).
4. Remove marcadores `_SUCCESS` e realiza upload limpo para `silver/ibge_uf/`.

---

### 🛒 c) Silver JSON Loja (`silver_json.py`)

Normaliza e estrutura diversos tipos de JSONs de uma loja fictícia:

- **dados_extrato** → `silver/json/transacoes/`
- **dados_pedidos** → `silver/json/pedidos_externos/` e `silver/json/pedidos_externos_itens/`
- **dados_produtos** → `silver/json/produtos_parceiros/`
- **dados_tags** → `silver/json/tags_produtos/`

Cada tipo de arquivo passa por uma rotina específica de *flatten*, *explode* e *cast*, gerando Parquets organizados por tema.

---

## 🧰 Dependências

Crie um ambiente virtual e instale os pacotes necessários:

```bash
pip install pyspark==3.5.0 pandas sqlalchemy==2.0.31 psycopg2-binary minio requests pyarrow
```

---

## 🐳 Execução no Docker / Dev Container

O projeto é compatível com ambientes baseados em **Docker Compose** contendo os serviços:

- `minio` → Object Storage (porta 9000)
- `db` → PostgreSQL
- `spark` → Container local para execução dos jobs Spark (opcional)

---

## 🚀 Ordem de Execução Recomendada

1. **Criação de infraestrutura**
   ```bash
   python create_buckets.py
   ```

2. **Cargas Bronze**
   ```bash
   python bronze_dbloja.py
   python bronze_ibge.py
   python bronze_json.py
   ```

3. **Transformações Silver**
   ```bash
   python silver_dbloja.py
   python silver_ibge.py
   python silver_json.py
   ```

---

## 🧭 Observações

- Todos os scripts são autônomos e idempotentes.
- A arquitetura pode ser estendida para camadas **Gold** e dashboards.
- Os dados ficam organizados conforme boas práticas de *data partitioning* e *schema enforcement*.
- A conexão com o MinIO é feita via endpoint `minio:9000` (não usar `localhost` dentro do Docker).

---

## 📊 Exemplo de Estrutura no MinIO

```
s3://datalake/
├── bronze/
│   ├── dbloja/
│   │   ├── produto/data=20251031/produto_20251031_102311.parquet
│   ├── api/
│   │   ├── data=20251031/ibge-uf_20251031_103455.json
│   └── json/
│       ├── dados_extrato/data=20251031/dados_extrato_20251031_100000.json
│
└── silver/
    ├── dbloja/
    ├── ibge_uf/
    └── json/
        ├── transacoes/
        ├── pedidos_externos/
        ├── produtos_parceiros/
        └── tags_produtos/
```

---

## 🧩 Próximos Passos (Extensões Sugeridas)

- Adicionar **camada Gold** (QuickSight / Power BI).
- Integrar com **Airflow** para orquestração.
- Implementar **Great Expectations** para validação de qualidade.
- Adicionar **versionamento de schema** e **CI/CD (GitHub Actions)**.
