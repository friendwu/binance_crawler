from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium_stealth import stealth
from webdriver_manager.chrome import ChromeDriverManager
from shutil import which


def create_webdriver():
    options = webdriver.ChromeOptions()
    options.headless = True
    options.add_argument("start-maximized")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    # service = ChromeService(ChromeDriverManager(cache_valid_range=10).install())
    # browser = webdriver.Chrome(options=options, service=service)
    browser = webdriver.Chrome(options=options, executable_path=which("chromedriver"))
    stealth(
        browser,
        languages=["en-US", "en"],
        vendor="Google Inc.",
        platform="Win32",
        webgl_vendor="Intel Inc.",
        renderer="Intel Iris OpenGL Engine",
        fix_hairline=True,
    )

    return browser


browser = create_webdriver()

#browser.get("https://www.stakingrewards.com/earn/ethereum-2-0/metrics/")
browser.get("https://data.binance.vision/?prefix=data/spot/monthly/klines")

text = browser.page_source
print(text)
# print browser.find_element()
