import csv
import io
import os
import zipfile
from datetime import datetime

import requests
import scrapy
from scrapy.exporters import CsvItemExporter
from scrapy.shell import inspect_response
from scrapy_selenium import SeleniumRequest
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from sqlalchemy import create_engine
import psycopg2 
from .klines_settings import *

import logging

logger = logging.getLogger(__name__)


def inspect(response, spider):
    inspect_response(response, spider)


class SpotKlinesItem(scrapy.Item):
    pair = scrapy.Field()
    open_time = scrapy.Field()
    open_datetime = scrapy.Field()
    open = scrapy.Field()
    high = scrapy.Field()
    low = scrapy.Field()
    close = scrapy.Field()
    volume = scrapy.Field()
    close_time = scrapy.Field()
    close_datetime = scrapy.Field()
    quote_asset_volume = scrapy.Field()
    num_of_trades = scrapy.Field()
    taker_buy_base_asset_volume = scrapy.Field()
    taker_buy_quote_asset_volume = scrapy.Field()
    ignore = scrapy.Field()
    candle_range = scrapy.Field()
    url = scrapy.Field()


class MyDupeFilter(object):
    def __init__(self, path, debug=False):
        self.debug = debug
        self.dupe_urls = set()
        self.filepath = os.path.join(path, "duplicate_urls")
        if os.path.exists(self.filepath):
            with open(self.filepath, "r") as f:
                for l in f:
                    self.dupe_urls.add(l.strip())

        self.file = open(self.filepath, "a+") 

    def duplicate(self, url):
        if url in self.dupe_urls:
            return True
        else:
            return False

    def update_dupe_url(self, url):
        if self.duplicate(url):
            return
        else:
            self.dupe_urls.add(url)
            self.file.write(url + "\n")
            self.file.flush()


class KlinesSpider(scrapy.Spider):
    name = "klines"

    custom_settings = {}

    def closed(self, reason):
        self.cur.close()
        self.conn.close()

    def start_requests(self):
        self.candle_range = CANDLE_RANGE
        self.root = DATA_ROOT
        self.pg_url = POSTGRESQL_URL
        self.table = POSTGRESQL_TABLE
        self.interval = INTERVAL
        print(self.root, DATA_ROOT)

        os.makedirs(self.root, exist_ok=True)
        self.engine = create_engine(self.pg_url)
        self.conn = self.engine.connect()
        self.cur = self.conn.connection.cursor()
        self.dupe_filter = MyDupeFilter("/root/binance_crawler/")


        yield SeleniumRequest(
            url=f"https://data.binance.vision/?prefix=data/spot/{self.interval}/klines",
            callback=self.parse,
            wait_time=10,
            wait_until=EC.presence_of_element_located(
                (By.CSS_SELECTOR, "#listing > tr > td > a")
            ),
            #  wait_until=EC.presence_of_element_located(
            #      (By.LINK_TEXT, "1INCHBTC/")
            #  ),  # FIXME.
            dont_filter=True,
        )

    def parse(self, response):
        for a in response.css("#listing > tr > td:nth-child(1) > a"):
            href = a.css("::attr(href)").get()
            pair = a.css("::text").get()[:-1]
            if f"spot/{self.interval}/klines/" in href and href.endswith("/"):
                url = response.urljoin(href) + f"{self.candle_range}/"

                # if self.dupe_filter.duplicate(href):
                #     logger.info(f"duplicate url: {href}")
                #     continue
                                
                yield SeleniumRequest(
                    url=url,
                    callback=self.parse_klines,
                    wait_time=10,
                    wait_until=EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "#listing > tr > td > a")
                    ),
                    meta={"pair": pair},
                    # dont_filter=True,
                )
                # break

    def parse_zipfile(self, response):
        # file_request = requests.get(href)
        zip_file = zipfile.ZipFile(io.BytesIO(response.body))
        csv_file = zip_file.open(zip_file.namelist()[0])
        csv_data = csv.reader(io.StringIO(csv_file.read().decode("utf-8")))

        items = []
        for row in csv_data:
            item = SpotKlinesItem()
            item["open_datetime"] = datetime.fromtimestamp(int(row[0]) / 1000)
            item["pair"] = response.meta["pair"]
            item["open_time"] = row[0]
            item["open"] = row[1]
            item["high"] = row[2]
            item["low"] = row[3]
            item["close"] = row[4]
            item["volume"] = row[5]
            item["close_time"] = row[6]
            item["close_datetime"] = datetime.fromtimestamp(int(row[6]) / 1000)
            item["quote_asset_volume"] = row[7]
            item["num_of_trades"] = row[8]
            item["taker_buy_base_asset_volume"] = row[9]
            item["taker_buy_quote_asset_volume"] = row[10]
            item["ignore"] = row[11]
            item["candle_range"] = self.candle_range
            item["url"] = response.url

            items.append(item)

        if self.export_kline_items(items, response.url):
            self.dupe_filter.update_dupe_url(response.url)
            # self.dupe_filter.update_dupe_url(response.url)


    def parse_klines(self, response):
        for href in response.css("a::attr(href)").getall():
            if not href.endswith(".zip"):
                continue

            if self.dupe_filter.duplicate(href):
                logger.info(f"duplicate url: {href}")
                continue

            logger.info(f"download zip file: {href}")

            yield scrapy.Request(
                url=response.urljoin(href),
                callback=self.parse_zipfile,
                meta={"pair": response.meta["pair"]},
            )
            

    def export_kline_items(self, items, url):
        csv_file = f"{self.root}/{url.split('/')[-1][:-4]}.csv"
        with open(csv_file, "wb") as f:
            exporter = CsvItemExporter(f)
            exporter.fields_to_export = [
                "open_time",
                "open_datetime",
                "pair",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "close_time",
                "close_datetime",
                "quote_asset_volume",
                "num_of_trades",
                "taker_buy_base_asset_volume",
                "taker_buy_quote_asset_volume",
                "candle_range",
                "url",
            ]
            exporter.start_exporting()
            _ = [exporter.export_item(item) for item in items]
            exporter.finish_exporting()

        with open(csv_file, "r") as f:
            copy_text = f"""COPY {self.table} (open_time, open_datetime, pair, open, high, low, close, volume, 
                                close_time, close_datetime, quote_asset_volume, num_of_trades, 
                                taker_buy_base_asset_volume, taker_buy_quote_asset_volume, candle_range, url) 
                                FROM STDIN WITH delimiter ',' CSV HEADER"""


            try:
                self.cur.copy_expert(copy_text, f)
            except psycopg2.Error as e:
                logger.warning(f"copy text: {copy_text}, {e.pgerror}, {e.diag.message_primary}, {e.diag.sqlstate}, {e.diag.context}, {e.diag.statement_position}, {e.diag.internal_position}, {e.diag.internal_query}")
                self.conn.connection.rollback()
                return False
            else:
                self.conn.connection.commit()
                return True
        
