import os
import pickle
from scrapy.dupefilters import RFPDupeFilter


class MyDupeFilter(RFPDupeFilter):
    def __init__(self, path, debug=False):
        super().__init__(path, debug)

        self.file = None
        self.urls_seen = set()
        self.filepath = os.path.join(path, "seen_urls.pkl")
        self.debug = debug
        if os.path.exists(self.filepath):
            with open(self.filepath, "rb") as f:
                self.urls_seen = pickle.load(f)

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        debug = settings.getbool("MY_DUPEFILTER_DEBUG")
        path = settings.get("MY_DUPEFILTER_FILE_PATH")
        return cls(path, debug)

    def request_seen(self, request):
        if request.url in self.urls_seen:
            return True
        self.urls_seen.add(request.url)

    def close(self, reason):
        super().close(reason)
        with open(self.filepath, "wb") as f:
            pickle.dump(self.urls_seen, f)
