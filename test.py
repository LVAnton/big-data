from selenium import webdriver
from selenium.common import NoSuchElementException
from selenium.webdriver.common.by import By
import time, re

CITIES = {"Москва": "moskva", "Санкт-Петербург": "sankt-peterburg"}
SEARCH_QUERY = "Data Engineer"

options = webdriver.ChromeOptions()
options.add_argument("--headless")  # без открытия окна
drv = webdriver.Chrome(options=options)

def safe_get(ad, selector, attr=None, to_num=False, to_float=False):
    try:
        el = ad.find_element(By.CSS_SELECTOR, selector)
        val = el.get_attribute(attr) if attr else el.text
        if to_num: return int("".join(str(c) for c in val if c.isdigit()))
        if to_float: return float(val.replace(',', '.'))
        return val.strip() if val else None
    except NoSuchElementException:
        return None

def parse_salary(salary_text: str):
    text = salary_text.replace("\xa0", " ").replace("\u202f", " ").strip()

    match = re.search(r'(\d[\d\s]*)\s*[—-]?\s*(\d[\d\s]*)?\s*(.*)', text)
    if match:
        salary_from = int(match.group(1).replace(" ", "")) if match.group(1) else None
        salary_to = int(match.group(2).replace(" ", "")) if match.group(2) else salary_from
        salary_type = match.group(3).strip() if match.group(3) else None
        return salary_from, salary_to, salary_type
    return None, None, None

for city, slug in CITIES.items():
    drv.get(f"https://www.avito.ru/{slug}/rabota?q=%22{SEARCH_QUERY}%22")
    time.sleep(3)

    ads = drv.find_elements(By.CSS_SELECTOR, "div.js-catalog-item-enum")
    print(f"{city}: найдено {len(ads)} вакансий")

    for ad in ads[:5]:
        url = safe_get(ad, "a[data-marker='item-title']", attr="href")
        salary = ad.find_element(By.CSS_SELECTOR, "p[data-marker='item-price'] span").text
        salary_from, salary_to, salary_type = parse_salary(salary)

        doc = {
            "url": url,
            "city": city,
            "title": safe_get(ad, "a[data-marker='item-title']"),
            "location": safe_get(ad, "div[data-marker='item-location'] p span:nth-of-type(2)"),
            "description": safe_get(
                ad,
                "div > div > div.iva-item-body-oMJBI > div:nth-child(4) > p"
            ),
            "salary": {
                "from": salary_from,
                "to": salary_to,
                "type": salary_type
            },
            "raw_html": ad.get_attribute("innerHTML")
        }

        print(f"🏙️ Город: {doc['city']}")
        print(f"🔗 URL: {doc['url']}")
        print(f"💼 Вакансия: {doc['title']}")
        if doc["salary"]["from"] and doc["salary"]["to"]:
            print(
                f"💰 Зарплата: {doc['salary']['from']:,} — {doc['salary']['to']:,} {doc['salary']['type'] or ''}".replace(
                    ',', ' '))
        elif doc["salary"]["from"]:
            print(f"💰 Зарплата от {doc['salary']['from']:,} {doc['salary']['type'] or ''}".replace(',', ' '))
        else:
            print("💰 Зарплата: не указана")
        print(f"📍 Место: {doc['location']}")
        print(f"📝 Описание: {doc['description'] or '—'}")
        print("-" * 80)

drv.quit()