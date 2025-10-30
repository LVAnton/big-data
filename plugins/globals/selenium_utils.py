from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common import NoSuchElementException
from selenium.webdriver.common.by import By


SELENIUM_URL = "http://selenium:4444/wd/hub"


def get_selenium_driver() -> webdriver.Remote:
    opts = Options();  opts.add_argument("--headless")
    opts.add_argument("--no-sandbox"); opts.add_argument("--disable-dev-shm-usage")
    return webdriver.Remote(command_executor=SELENIUM_URL, options=opts)


def safe_get(ad, selector, attr=None, to_num=False, to_float=False):
    try:
        el = ad.find_element(By.CSS_SELECTOR, selector)
        val = el.get_attribute(attr) if attr else el.text
        if to_num: return int("".join(str(c) for c in val if c.isdigit()))
        if to_float: return float(val.replace(',', '.'))
        return val.strip() if val else None
    except NoSuchElementException:
        return None

