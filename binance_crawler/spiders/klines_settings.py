TYPE = "spot"
CANDLE_RANGE = "4h"
INTERVAL = "daily"
POSTGRESQL_TABLE = f"blockchain_overview.binance_spot_klines_{CANDLE_RANGE}"
DATA_ROOT = f"/root/binance_crawler/data/spot/{INTERVAL}/klines/{CANDLE_RANGE}/"
POSTGRESQL_URL=<your pg url>
