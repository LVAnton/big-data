from __future__ import annotations
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from typing import List, Dict, Any
import time
import json
import random
import string
import pandas as pd
import matplotlib.pyplot as plt

DATASET_SIZE = 2000
POSTGRES_TABLE = "vacancies"
MONGO_DB = "bench"
MONGO_COLL = "vacancies"
MINIO_BUCKET = "vacancies"
MINIO_PREFIX = "vacancies/"

RESULTS_BUCKET = "bench-results"
RESULTS_PREFIX = "results/"

from globals import get_minio_client, get_postgres_client, get_mongo_client


def make_record(i: int) -> Dict[str, Any]:
    return {
        "id": i,
        "title": "Job " + ''.join(random.choices(string.ascii_uppercase + string.digits, k=6)),
        "salary_from": random.randint(30000, 100000),
        "salary_to": random.randint(100001, 200000),
        "salary_type": random.choice(["annual", "monthly", "hourly"]),
        "employer": random.choice(["Google", "Amazon", "Yandex", "VK", "Ozon"]),
        "city": random.choice(["Moscow", "London", "Berlin", "Amsterdam"]),
    }

def to_serializable(doc: Dict[str, Any]) -> Dict[str, Any]:
    return {k: str(v) if str(type(v)).endswith("ObjectId'>") else v for k, v in doc.items()}

@dag(
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["benchmark", "dbs"],
)
def dbs_benchmark():

    @task
    def generate_data(n: int = DATASET_SIZE) -> List[Dict[str, Any]]:
        return [make_record(i) for i in range(1, n + 1)]

    @task
    def prepare_backends() -> dict:
        pg = get_postgres_client()
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {POSTGRES_TABLE} (
            id BIGINT PRIMARY KEY,
            title TEXT,
            salary_from DOUBLE PRECISION,
            salary_to DOUBLE PRECISION,
            salary_type TEXT,
            employer TEXT,
            city TEXT
        );
        """
        pg.run(create_sql)

        mongo = get_mongo_client()
        mongo.create_database(MONGO_DB)
        mongo.create_collection(MONGO_DB, MONGO_COLL)

        minio = get_minio_client()
        if not minio.bucket_exists(MINIO_BUCKET):
            minio.create_bucket(MINIO_BUCKET)
        if not minio.bucket_exists(RESULTS_BUCKET):
            minio.create_bucket(RESULTS_BUCKET)

        return {"ok": True}

    @task
    def benchmark_writes(records: List[Dict[str, Any]]) -> Dict[str, float]:
        timings = {}

        pg = get_postgres_client()
        pg.run(f"DELETE FROM {POSTGRES_TABLE}")
        t0 = time.perf_counter()
        for r in records:
            pg.insert(POSTGRES_TABLE, r)
        timings['postgres_write'] = time.perf_counter() - t0

        mongo = get_mongo_client()
        mongo._db(MONGO_DB)[MONGO_COLL].delete_many({})
        t0 = time.perf_counter()
        for r in records:
            mongo.insert_one(MONGO_DB, MONGO_COLL, r)
        timings['mongo_write'] = time.perf_counter() - t0

        minio = get_minio_client()
        t0 = time.perf_counter()
        for r in records:
            key = f"{MINIO_PREFIX}vacancy_{r['id']}.json"
            minio.upload_bytes(json.dumps(to_serializable(r)).encode('utf-8'), MINIO_BUCKET, key)
        timings['minio_write'] = time.perf_counter() - t0

        return timings

    @task
    def benchmark_reads(write_done) -> Dict[str, float]:
        timings = {}

        pg = get_postgres_client()
        t0 = time.perf_counter()
        _ = pg.select(POSTGRES_TABLE)
        timings['postgres_read'] = time.perf_counter() - t0

        mongo = get_mongo_client()
        t0 = time.perf_counter()
        _ = mongo.find(MONGO_DB, MONGO_COLL, {})
        timings['mongo_read'] = time.perf_counter() - t0

        minio = get_minio_client()
        t0 = time.perf_counter()
        client = minio.client
        paginator = client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=MINIO_BUCKET, Prefix=MINIO_PREFIX):
            for obj in page.get('Contents', []) or []:
                resp = client.get_object(Bucket=MINIO_BUCKET, Key=obj['Key'])
                _ = resp['Body'].read()
        timings['minio_read'] = time.perf_counter() - t0

        return timings

    @task
    def save_results(write_timings: Dict[str, float], read_timings: Dict[str, float]) -> Dict[str, str]:
        write_labels = ['Postgres', 'Mongo', 'Minio']
        write_values = [write_timings['postgres_write'], write_timings['mongo_write'], write_timings['minio_write']]
        read_labels = ['Postgres', 'Mongo', 'Minio']
        read_values = [read_timings['postgres_read'], read_timings['mongo_read'], read_timings['minio_read']]

        df = pd.DataFrame({
            'backend': write_labels + read_labels,
            'operation': ['write']*3 + ['read']*3,
            'seconds': write_values + read_values,
        })
        csv_bytes = df.to_csv(index=False).encode('utf-8')

        plt.figure(figsize=(8,4))
        plt.bar(write_labels, write_values)
        plt.title(f'Write benchmark (n={DATASET_SIZE})')
        plt.ylabel('seconds')
        plt.tight_layout()
        plt.savefig("/tmp/write.png")  # temp save
        plt.close()
        with open("/tmp/write.png", "rb") as f:
            write_bytes = f.read()

        plt.figure(figsize=(8,4))
        plt.bar(read_labels, read_values)
        plt.title(f'Read benchmark (n={DATASET_SIZE})')
        plt.ylabel('seconds')
        plt.tight_layout()
        plt.savefig("/tmp/read.png")
        plt.close()
        with open("/tmp/read.png", "rb") as f:
            read_bytes = f.read()

        minio = get_minio_client()
        minio.upload_bytes(csv_bytes, RESULTS_BUCKET, f"{RESULTS_PREFIX}bench_timings.csv")
        minio.upload_bytes(write_bytes, RESULTS_BUCKET, f"{RESULTS_PREFIX}bench_write.png")
        minio.upload_bytes(read_bytes, RESULTS_BUCKET, f"{RESULTS_PREFIX}bench_read.png")

        return {
            "csv": f"s3://{RESULTS_BUCKET}/{RESULTS_PREFIX}bench_timings.csv",
            "write_graph": f"s3://{RESULTS_BUCKET}/{RESULTS_PREFIX}bench_write.png",
            "read_graph": f"s3://{RESULTS_BUCKET}/{RESULTS_PREFIX}bench_read.png"
        }

    data = generate_data()
    prepare_backends()
    write_times = benchmark_writes(data)
    read_times = benchmark_reads(write_times)
    results = save_results(write_times, read_times)

    return results

dbs_benchmark()
