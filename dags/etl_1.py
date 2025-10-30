from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests, logging, time
import boto3
from globals import get_minio_client, get_postgres_client
from globals.selenium_utils import *

CITIES = {"Москва": "moskva", "Санкт-Петербург": "sankt-peterburg"}
BUCKET = "iphone-17-pro-max-cheap"

@dag(default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime.today(),
        'retries': 1,
        'retry_delay': timedelta(minutes=2)
    },
    schedule_interval='@daily',
    catchup=False,
    tags=['avito', 'iphone'])
def iphone_17_pro_max_cheap_etl():

    @task()
    def scrape_ads():
        logging.info("Start scraping iPhone 17 Pro Max ads")

        drv = get_selenium_driver()

        results = []
        for city_name, slug in CITIES.items():
            drv.get(f"https://www.avito.ru/{slug}/telefony?q=iPhone+17+Pro+Max&s=1")
            time.sleep(3)
            ads = drv.find_elements(By.CSS_SELECTOR, "div.js-catalog-item-enum")
            logging.info(f"{city_name}: найдено {len(ads)} объявлений")

            for ad in ads[:5]:
                try:
                    t = safe_get(ad, "a[data-marker='item-title']")
                    u = safe_get(ad, "a[data-marker='item-title']", attr="href")
                    p = safe_get(ad, "p[data-marker='item-price'] strong span", to_num=True)
                    img = safe_get(ad, "img", attr="src")
                    k = f"{slug}/{hash(u)}.jpg"

                    results.append({
                        "title": t,
                        "city": city_name,
                        "url": u,
                        "price": p,
                        "img": img,
                        "img_key": k,
                        "location": safe_get(ad, "div[data-marker='item-location'] p span:nth-of-type(2)"),
                        "is_new": safe_get(ad, "p[data-marker='item-specific-params']") == "Новый",
                        "seller_score": safe_get(ad, "span[data-marker='seller-info/score']", to_float=True),
                        "seller_rates": safe_get(ad, "p[data-marker='seller-info/summary']", to_num=True),
                        "description": safe_get(ad, "div:nth-child(2) > div:nth-child(1) > div:nth-child(2) > div:nth-child(4) > div:nth-child(1) > p:nth-child(1)")
                    })
                except Exception as e:
                    logging.warning(f"Ошибка при обработке объявления: {e}")

        drv.quit()
        return results

    @task()
    def save_ads(ads_list: list):
        pg = get_postgres_client()
        s3 = get_minio_client()

        for ad in ads_list:
            if pg.select(table="iphone_ads", columns=["id"], where="url=%s", params=(ad["url"],)):
                logging.info(f"Объявление уже есть в базе: {ad['url']}")
                continue

            if ad["img"]: s3.upload_bytes(requests.get(ad["img"]).content, BUCKET, ad["img_key"])

            pg.insert("iphone_ads", {k: v for k, v in ad.items() if k != "img"})

    save_ads(scrape_ads())

iphone_17_pro_max_cheap_etl()
