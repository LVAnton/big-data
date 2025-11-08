from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests, logging, time, json, hashlib
from globals import get_minio_client, get_postgres_client, get_mongo_client
from globals.selenium_utils import get_selenium_driver, safe_get
from selenium.webdriver.common.by import By

CITIES = {"Москва": "moskva", "Санкт-Петербург": "sankt-peterburg"}
BUCKET = "iphone-17-pro-max-cheap"
MONGO_DB, MONGO_COLL = "ods_mongo_db", "iphone_ads"

@dag(
    default_args={'owner': 'airflow', 'start_date': datetime.today(), 'retries': 1, 'retry_delay': timedelta(minutes=2)},
    schedule_interval='@daily', catchup=False, tags=['avito', 'iphone']
)
def iphone_17_pro_max_cheap_etl():

    @task()
    def scrape_and_save():
        pg, s3, mongo, drv = get_postgres_client(), get_minio_client(), get_mongo_client(), get_selenium_driver()

        for city, slug in CITIES.items():
            drv.get(f"https://www.avito.ru/{slug}/telefony?q=iPhone+17+Pro+Max&s=1")
            time.sleep(3)
            ads = drv.find_elements(By.CSS_SELECTOR, "div.js-catalog-item-enum")
            logging.info(f"{city}: найдено {len(ads)} объявлений")

            for ad in ads[:20]:
                try:
                    url = safe_get(ad, "a[data-marker='item-title']", attr="href")
                    if not url: continue

                    doc = {
                        "url": url,
                        "city": city,
                        "title": safe_get(ad, "a[data-marker='item-title']"),
                        "price": safe_get(ad, "p[data-marker='item-price'] strong span", to_num=True),
                        "img_key": f"{slug}/{hash(url)}.jpg",
                        "location": safe_get(ad, "div[data-marker='item-location'] p span:nth-of-type(2)"),
                        "is_new": safe_get(ad, "p[data-marker='item-specific-params']") == "Новый",
                        "seller_score": safe_get(ad, "span[data-marker='seller-info/score']", to_float=True),
                        "seller_rates": safe_get(ad, "p[data-marker='seller-info/summary']", to_num=True),
                        "description": safe_get(ad, "div:nth-child(2) > div:nth-child(1) > div:nth-child(2) > div:nth-child(4) > div:nth-child(1) > p:nth-child(1)"),
                        "raw_html": ad.get_attribute('innerHTML')
                    }

                    mongo.upsert_one(MONGO_DB, MONGO_COLL, {"url": url}, doc)
                    s3.upsert_object(BUCKET, f"raw/{hashlib.md5(url.encode()).hexdigest()}.json", json.dumps(doc).encode())
                    img = safe_get(ad, "img", attr="src")
                    if img: s3.upsert_object(BUCKET, f"img/{doc["img_key"]}", requests.get(img).content)

                    pg.upsert("iphone_ads", {k: v for k, v in doc.items() if k != "raw_html"}, conflict_column="url")

                except Exception as e:
                    logging.warning(f"Ошибка объявления: {e}")

        drv.quit()

    scrape_and_save()

iphone_17_pro_max_cheap_etl()