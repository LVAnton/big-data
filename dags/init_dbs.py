from airflow.decorators import dag, task
from datetime import datetime
from globals import get_postgres_client, get_minio_client, get_mongo_client

BUCKETS = [
    "iphone-17-pro-max-cheap",
    "hh-vacancies",
    "avito-vacancies"
]

COLLECTIONS = [
    "iphone_ads",
    "hh_vacancies",
    "avito_vacancies"
]

@dag(default_args={'owner': 'airflow', 'start_date': datetime.today()},
     schedule_interval=None,
     catchup=False)
def init_db():
    @task()
    def create_pg_tables():
        pg = get_postgres_client()
        pg.run(open("/opt/airflow/sql/pg/create_tables.sql").read())

    @task()
    def create_minio_bucket():
        s3 = get_minio_client()
        for bucket in BUCKETS:
            if not s3.bucket_exists(bucket=bucket):
                print(f"Bucket '{bucket}' not found — creating...")
                s3.create_bucket(bucket=bucket)

    @task()
    def create_mongo_collections():
        mongo = get_mongo_client()
        for collection in COLLECTIONS: mongo.create_collection("ods_mongo_db", collection)

    create_pg_tables()
    create_mongo_collections()
    create_minio_bucket()

init_db()
