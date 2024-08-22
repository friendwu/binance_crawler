# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import os

from itemadapter import ItemAdapter
from scrapy.exceptions import NotConfigured
from scrapy.exporters import CsvItemExporter
from sqlalchemy import create_engine


class SpotKlinesPipeline:
    def __init__(self, custom_settings):
        self.custom_settings = custom_settings

    @classmethod
    def from_crawler(cls, crawler):
        # get the value of MY_CUSTOM_SETTING
        custom_setting = crawler.settings.get("SPOT_KLINES_ITEM_PIPELINE_SETTINGS")

        # if custom_setting is None, raise an error
        if not custom_setting:
            raise NotConfigured("ITEM_PIPELINE_SETTINGS must be set")

        # instantiate the pipeline with the custom setting
        return cls(custom_setting)

    def open_spider(self, spider):
        self.exporter = {}

        self.root = self.custom_settings.get("DATA_ROOT")
        self.pg_url = self.custom_settings.get("POSTGRESQL_URL")
        self.table = self.custom_settings.get("POSTGRESQL_TABLE")

        os.makedirs(self.root, exist_ok=True)

    def close_spider(self, spider):
        engine = create_engine(self.pg_url)
        conn = engine.connect()
        cur = conn.connection.cursor()

        for exporter, csv_file, csv_filename in self.exporter.values():
            exporter.finish_exporting()
            csv_file.close()

            with open(csv_filename, "r") as f:
                # next(f)  # Skip the header row.
                copy_text = f"""COPY {self.table} (open_time, open_datetime, pair, open, high, low, close, volume,
                                    close_time, close_datetime, quote_asset_volume, num_of_trades,
                                    taker_buy_base_asset_volume, taker_buy_quote_asset_volume, candle_range, url)
                                    FROM STDIN WITH delimiter ',' CSV HEADER"""

                #
                cur.copy_expert(copy_text, f)
                conn.connection.commit()

            cur.close()
            conn.close()

    def _exporter_for_item(self, item):
        adapter = ItemAdapter(item)
        csv_filename = f"{self.root}/{adapter['url'].split('/')[-1][:-4]}.csv"
        if csv_filename not in self.exporter:
            csv_file = open(csv_filename, "wb")
            exporter = CsvItemExporter(csv_file)
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
            self.exporter[csv_filename] = (exporter, csv_file, csv_filename)

        return self.exporter[csv_filename][0]

    def process_item(self, item, spider):
        exporter = self._exporter_for_item(item)
        exporter.export_item(item)

        return item
