CREATE TABLE IF NOT EXISTS "blockchain_overview"."binance_spot_klines_5m" (
	"id" BIGSERIAL NOT NULL,
	"open_time" bigint NOT NULL,
	"open_datetime" timestamp NOT NULL,
	"pair" text,
	"open" numeric,
	"high" numeric,
	"low" numeric,
	"close" numeric,
	"volume" numeric,
	"close_time" bigint NOT NULL,
	"close_datetime" timestamp NOT NULL,

	"quote_asset_volume" numeric,
	"num_of_trades" bigint,
	"taker_buy_base_asset_volume" numeric,
	"taker_buy_quote_asset_volume" numeric,
	"candle_range" text,
	"url" text,
	"last_updated" timestamp,

	PRIMARY KEY ("pair", "open_time", "candle_range")
);


CREATE TABLE IF NOT EXISTS "blockchain_overview"."binance_spot_klines_4h" (
	"id" BIGSERIAL NOT NULL,
	"open_time" bigint NOT NULL,
	"open_datetime" timestamp NOT NULL,
	"pair" text,
	"open" numeric,
	"high" numeric,
	"low" numeric,
	"close" numeric,
	"volume" numeric,
	"close_time" bigint NOT NULL,
	"close_datetime" timestamp NOT NULL,

	"quote_asset_volume" numeric,
	"num_of_trades" bigint,
	"taker_buy_base_asset_volume" numeric,
	"taker_buy_quote_asset_volume" numeric,
	"candle_range" text,
	"url" text,
	"last_updated" timestamp,

	PRIMARY KEY ("pair", "open_time", "candle_range")
);
