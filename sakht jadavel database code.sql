-- Table: public.daily_indicators

-- DROP TABLE IF EXISTS public.daily_indicators;

CREATE TABLE IF NOT EXISTS public.daily_indicators
(
    stock_ticker text COLLATE pg_catalog."default" NOT NULL,
    date_miladi date NOT NULL,
    ema_20 double precision,
    ema_50 double precision,
    ema_100 double precision,
    rsi double precision,
    macd double precision,
    macd_signal double precision,
    macd_hist double precision,
    tenkan double precision,
    kijun double precision,
    senkou_a double precision,
    senkou_b double precision,
    chikou double precision,
    signal_ichimoku_buy smallint,
    signal_ichimoku_sell smallint,
    signal_ema_cross_buy smallint,
    signal_ema_cross_sell smallint,
    signal_rsi_buy smallint,
    signal_rsi_sell smallint,
    signal_macd_buy smallint,
    signal_macd_sell smallint,
    signal_ema50_100_buy smallint,
    signal_ema50_100_sell smallint,
    atr_22 double precision,
    renko_22 text COLLATE pg_catalog."default",
    ema_20_d numeric,
    ema_50_d numeric,
    ema_100_d numeric,
    rsi_d numeric,
    macd_d numeric,
    macd_signal_d numeric,
    macd_hist_d numeric,
    tenkan_d numeric,
    kijun_d numeric,
    senkou_a_d numeric,
    senkou_b_d numeric,
    chikou_d numeric,
    signal_ichimoku_buy_d integer,
    signal_ichimoku_sell_d integer,
    signal_ema_cross_buy_d integer,
    signal_ema_cross_sell_d integer,
    signal_rsi_buy_d integer,
    signal_rsi_sell_d integer,
    signal_macd_buy_d integer,
    signal_macd_sell_d integer,
    signal_ema50_100_buy_d integer,
    signal_ema50_100_sell_d integer,
    atr_22_d numeric,
    renko_22_d text COLLATE pg_catalog."default",
    CONSTRAINT daily_indicators_pkey PRIMARY KEY (stock_ticker, date_miladi)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.daily_indicators
    OWNER to postgres;

-- Table: public.daily_joined_data

-- DROP TABLE IF EXISTS public.daily_joined_data;

CREATE TABLE IF NOT EXISTS public.daily_joined_data
(
    id integer,
    stock_ticker text COLLATE pg_catalog."default",
    j_date character(10) COLLATE pg_catalog."default",
    date_miladi date,
    weekday text COLLATE pg_catalog."default",
    open integer,
    high integer,
    low integer,
    close integer,
    final_price integer,
    volume bigint,
    value bigint,
    trade_count integer,
    name text COLLATE pg_catalog."default",
    market text COLLATE pg_catalog."default",
    adjust_open integer,
    adjust_high integer,
    adjust_low integer,
    adjust_close integer,
    adjust_final_price integer,
    adjust_open_usd double precision,
    adjust_high_usd double precision,
    adjust_low_usd double precision,
    adjust_close_usd double precision,
    value_usd double precision,
    dollar_rate double precision,
    is_temp boolean,
    ema_20 double precision,
    ema_50 double precision,
    ema_100 double precision,
    rsi double precision,
    macd double precision,
    macd_signal double precision,
    macd_hist double precision,
    tenkan double precision,
    kijun double precision,
    senkou_a double precision,
    senkou_b double precision,
    chikou double precision,
    signal_ichimoku_buy smallint,
    signal_ichimoku_sell smallint,
    signal_ema_cross_buy smallint,
    signal_ema_cross_sell smallint,
    signal_rsi_buy smallint,
    signal_rsi_sell smallint,
    signal_macd_buy smallint,
    signal_macd_sell smallint,
    signal_ema50_100_buy smallint,
    signal_ema50_100_sell smallint,
    atr_22 double precision,
    renko_22 text COLLATE pg_catalog."default",
    signal_ichimoku_buy_usd integer,
    signal_ichimoku_sell_usd integer,
    signal_ema_cross_buy_usd integer,
    signal_ema_cross_sell_usd integer,
    signal_rsi_buy_usd integer,
    signal_rsi_sell_usd integer,
    signal_macd_buy_usd integer,
    signal_macd_sell_usd integer,
    signal_ema50_100_buy_usd integer,
    signal_ema50_100_sell_usd integer,
    renko_22_usd text COLLATE pg_catalog."default",
    inscode text COLLATE pg_catalog."default",
    buy_i_volume double precision,
    buy_n_volume double precision,
    buy_i_value double precision,
    buy_n_value double precision,
    buy_n_count integer,
    sell_i_volume double precision,
    buy_i_count integer,
    sell_n_volume double precision,
    sell_i_value double precision,
    sell_n_value double precision,
    sell_n_count integer,
    sell_i_count integer,
    buy_i_value_usd double precision,
    buy_n_value_usd double precision,
    sell_i_value_usd double precision,
    sell_n_value_usd double precision,
    name_en text COLLATE pg_catalog."default",
    sector text COLLATE pg_catalog."default",
    sector_code bigint,
    subsector text COLLATE pg_catalog."default",
    market2 text COLLATE pg_catalog."default",
    panel text COLLATE pg_catalog."default",
    share_number bigint,
    base_vol bigint,
    "instrumentID" text COLLATE pg_catalog."default",
    marketcap bigint,
    marketcap_usd double precision
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.daily_joined_data
    OWNER to postgres;

-- Table: public.daily_market_summary

-- DROP TABLE IF EXISTS public.daily_market_summary;

CREATE TABLE IF NOT EXISTS public.daily_market_summary
(
    date date NOT NULL,
    stock_ticker text COLLATE pg_catalog."default" NOT NULL,
    sector text COLLATE pg_catalog."default",
    value bigint,
    market_cap bigint,
    value_usd double precision,
    market_cap_usd double precision,
    CONSTRAINT daily_market_summary_pkey PRIMARY KEY (date, stock_ticker)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.daily_market_summary
    OWNER to postgres;

-- Table: public.daily_stock_data

-- DROP TABLE IF EXISTS public.daily_stock_data;

CREATE TABLE IF NOT EXISTS public.daily_stock_data
(
    id integer NOT NULL DEFAULT nextval('daily_stock_data_id_seq'::regclass),
    stock_ticker text COLLATE pg_catalog."default",
    j_date character(10) COLLATE pg_catalog."default",
    date_miladi date,
    weekday text COLLATE pg_catalog."default",
    open integer,
    high integer,
    low integer,
    close integer,
    final_price integer,
    volume bigint,
    value bigint,
    trade_count integer,
    name text COLLATE pg_catalog."default",
    market text COLLATE pg_catalog."default",
    adjust_open integer,
    adjust_high integer,
    adjust_low integer,
    adjust_close integer,
    adjust_final_price integer,
    adjust_open_usd double precision,
    adjust_high_usd double precision,
    adjust_low_usd double precision,
    adjust_close_usd double precision,
    value_usd double precision,
    dollar_rate double precision,
    is_temp boolean DEFAULT false,
    CONSTRAINT daily_stock_data_pkey PRIMARY KEY (id),
    CONSTRAINT daily_stock_data_stock_ticker_date_miladi_key UNIQUE (stock_ticker, date_miladi)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.daily_stock_data
    OWNER to postgres;

-- Table: public.dollar_data

-- DROP TABLE IF EXISTS public.dollar_data;

CREATE TABLE IF NOT EXISTS public.dollar_data
(
    date_miladi date NOT NULL,
    open double precision,
    high double precision,
    low double precision,
    close double precision,
    CONSTRAINT dollar_data_pkey PRIMARY KEY (date_miladi)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.dollar_data
    OWNER to postgres;

-- Table: public.haghighi

-- DROP TABLE IF EXISTS public.haghighi;

CREATE TABLE IF NOT EXISTS public.haghighi
(
    recdate date,
    inscode text COLLATE pg_catalog."default",
    buy_i_volume double precision,
    buy_n_volume double precision,
    buy_i_value double precision,
    buy_n_value double precision,
    buy_n_count integer,
    sell_i_volume double precision,
    buy_i_count integer,
    sell_n_volume double precision,
    sell_i_value double precision,
    sell_n_value double precision,
    sell_n_count integer,
    sell_i_count integer,
    symbol text COLLATE pg_catalog."default",
    dollar_rate numeric,
    buy_i_value_usd double precision,
    buy_n_value_usd double precision,
    sell_i_value_usd double precision,
    sell_n_value_usd double precision,
    sector text COLLATE pg_catalog."default",
    is_temp boolean DEFAULT false,
    CONSTRAINT haghighi_symbol_recdate_unique UNIQUE (symbol, recdate)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.haghighi
    OWNER to postgres;

-- Table: public.live_market_data

-- DROP TABLE IF EXISTS public.live_market_data;

CREATE TABLE IF NOT EXISTS public.live_market_data
(
    "Ticker" text COLLATE pg_catalog."default" NOT NULL,
    "Trade Type" text COLLATE pg_catalog."default",
    "Time" text COLLATE pg_catalog."default",
    "Open" numeric,
    "High" numeric,
    "Low" numeric,
    "Close" numeric,
    "Final" numeric,
    "Close(%)" numeric,
    "Final(%)" numeric,
    "Day_UL" numeric,
    "Day_LL" numeric,
    "Value" bigint,
    "BQ-Value" bigint,
    "SQ-Value" bigint,
    "BQPC" numeric,
    "SQPC" numeric,
    "Volume" bigint,
    "Vol_Buy_R" bigint,
    "Vol_Buy_I" bigint,
    "Vol_Sell_R" bigint,
    "Vol_Sell_I" bigint,
    "No" integer,
    "No_Buy_R" integer,
    "No_Buy_I" integer,
    "No_Sell_R" integer,
    "No_Sell_I" integer,
    "Name" text COLLATE pg_catalog."default",
    "Market" text COLLATE pg_catalog."default",
    "Sector" text COLLATE pg_catalog."default",
    "Share-No" bigint,
    "Base-Vol" bigint,
    "Market Cap" bigint,
    "EPS" numeric,
    "Download" timestamp without time zone NOT NULL,
    updated_at timestamp without time zone,
    CONSTRAINT live_market_data_pkey PRIMARY KEY ("Ticker", "Download")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.live_market_data
    OWNER to postgres;

-- Table: public.orderbook_snapshot

-- DROP TABLE IF EXISTS public.orderbook_snapshot;

CREATE TABLE IF NOT EXISTS public.orderbook_snapshot
(
    "insCode" bigint,
    "Symbol" text COLLATE pg_catalog."default",
    "Timestamp" timestamp without time zone,
    "BuyPrice1" numeric,
    "BuyVolume1" bigint,
    "SellPrice1" numeric,
    "SellVolume1" bigint,
    "BuyPrice2" numeric,
    "BuyVolume2" bigint,
    "SellPrice2" numeric,
    "SellVolume2" bigint,
    "BuyPrice3" numeric,
    "BuyVolume3" bigint,
    "SellPrice3" numeric,
    "SellVolume3" bigint,
    "BuyPrice4" numeric,
    "BuyVolume4" bigint,
    "SellPrice4" numeric,
    "SellVolume4" bigint,
    "BuyPrice5" numeric,
    "BuyVolume5" bigint,
    "SellPrice5" numeric,
    "SellVolume5" bigint,
    "Sector" text COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.orderbook_snapshot
    OWNER to postgres;

 -- Table: public.quote

-- DROP TABLE IF EXISTS public.quote;

CREATE TABLE IF NOT EXISTS public.quote
(
    "Day_UL" bigint,
    "Day_LL" bigint,
    "Value" bigint,
    "Time" text COLLATE pg_catalog."default",
    "BQ_Value" bigint,
    "SQ_Value" bigint,
    "BQPC" bigint,
    "SQPC" bigint,
    stock_ticker text COLLATE pg_catalog."default",
    date text COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.quote
    OWNER to postgres;

-- Table: public.symboldetail

-- DROP TABLE IF EXISTS public.symboldetail;

CREATE TABLE IF NOT EXISTS public.symboldetail
(
    "insCode" bigint,
    name text COLLATE pg_catalog."default",
    name_en text COLLATE pg_catalog."default",
    sector text COLLATE pg_catalog."default",
    sector_code bigint,
    subsector text COLLATE pg_catalog."default",
    market text COLLATE pg_catalog."default",
    panel text COLLATE pg_catalog."default",
    stock_ticker text COLLATE pg_catalog."default",
    share_number bigint,
    base_vol bigint,
    "instrumentID" text COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.symboldetail
    OWNER to postgres;

-- Table: public.weekly_haghighi

-- DROP TABLE IF EXISTS public.weekly_haghighi;

CREATE TABLE IF NOT EXISTS public.weekly_haghighi
(
    id integer NOT NULL DEFAULT nextval('weekly_haghighi_id_seq'::regclass),
    symbol text COLLATE pg_catalog."default",
    week_start date,
    week_end date,
    buy_i_volume bigint,
    buy_n_volume bigint,
    buy_i_value bigint,
    buy_n_value bigint,
    buy_n_count bigint,
    sell_i_volume bigint,
    buy_i_count bigint,
    sell_n_volume bigint,
    sell_i_value bigint,
    sell_n_value bigint,
    sell_n_count bigint,
    sell_i_count bigint,
    buy_i_value_usd numeric,
    buy_n_value_usd numeric,
    sell_i_value_usd numeric,
    sell_n_value_usd numeric,
    CONSTRAINT weekly_haghighi_pkey PRIMARY KEY (id),
    CONSTRAINT weekly_haghighi_symbol_week_end_key UNIQUE (symbol, week_end)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.weekly_haghighi
    OWNER to postgres;

-- Table: public.weekly_indicators

-- DROP TABLE IF EXISTS public.weekly_indicators;

CREATE TABLE IF NOT EXISTS public.weekly_indicators
(
    stock_ticker text COLLATE pg_catalog."default" NOT NULL,
    week_end date NOT NULL,
    ema_20 double precision,
    ema_50 double precision,
    ema_100 double precision,
    rsi double precision,
    macd double precision,
    macd_signal double precision,
    macd_hist double precision,
    tenkan double precision,
    kijun double precision,
    senkou_a double precision,
    senkou_b double precision,
    chikou double precision,
    signal_ichimoku_buy smallint,
    signal_ichimoku_sell smallint,
    signal_ema_cross_buy smallint,
    signal_ema_cross_sell smallint,
    signal_rsi_buy smallint,
    signal_rsi_sell smallint,
    signal_macd_buy smallint,
    signal_macd_sell smallint,
    signal_ema50_100_buy smallint,
    signal_ema50_100_sell smallint,
    atr_52 numeric,
    renko_52 text COLLATE pg_catalog."default",
    ema_20_d numeric,
    ema_50_d numeric,
    ema_100_d numeric,
    rsi_d numeric,
    macd_d numeric,
    macd_signal_d numeric,
    macd_hist_d numeric,
    tenkan_d numeric,
    kijun_d numeric,
    senkou_a_d numeric,
    senkou_b_d numeric,
    chikou_d numeric,
    signal_ichimoku_buy_d integer,
    signal_ichimoku_sell_d integer,
    signal_ema_cross_buy_d integer,
    signal_ema_cross_sell_d integer,
    signal_rsi_buy_d integer,
    signal_rsi_sell_d integer,
    signal_macd_buy_d integer,
    signal_macd_sell_d integer,
    signal_ema50_100_buy_d integer,
    signal_ema50_100_sell_d integer,
    atr_52_d numeric,
    renko_52_d text COLLATE pg_catalog."default",
    CONSTRAINT weekly_indicators_pkey PRIMARY KEY (stock_ticker, week_end)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.weekly_indicators
    OWNER to postgres;

 -- Table: public.weekly_joined_data

-- DROP TABLE IF EXISTS public.weekly_joined_data;

CREATE TABLE IF NOT EXISTS public.weekly_joined_data
(
    id integer,
    stock_ticker text COLLATE pg_catalog."default",
    week_start date,
    week_end date,
    open integer,
    high integer,
    low integer,
    close integer,
    final_price integer,
    adjust_open integer,
    adjust_high integer,
    adjust_low integer,
    adjust_close integer,
    adjust_final_price integer,
    volume bigint,
    value bigint,
    name text COLLATE pg_catalog."default",
    market text COLLATE pg_catalog."default",
    adjust_open_usd numeric,
    adjust_high_usd numeric,
    adjust_low_usd numeric,
    adjust_close_usd numeric,
    value_usd numeric,
    dollar_rate numeric,
    ema_20 double precision,
    ema_50 double precision,
    ema_100 double precision,
    rsi double precision,
    macd double precision,
    macd_signal double precision,
    macd_hist double precision,
    tenkan double precision,
    kijun double precision,
    senkou_a double precision,
    senkou_b double precision,
    chikou double precision,
    signal_ichimoku_buy smallint,
    signal_ichimoku_sell smallint,
    signal_ema_cross_buy smallint,
    signal_ema_cross_sell smallint,
    signal_rsi_buy smallint,
    signal_rsi_sell smallint,
    signal_macd_buy smallint,
    signal_macd_sell smallint,
    signal_ema50_100_buy smallint,
    signal_ema50_100_sell smallint,
    atr_52 numeric,
    renko_52 text COLLATE pg_catalog."default",
    ema_20_d numeric,
    ema_50_d numeric,
    ema_100_d numeric,
    rsi_d numeric,
    macd_d numeric,
    macd_signal_d numeric,
    macd_hist_d numeric,
    tenkan_d numeric,
    kijun_d numeric,
    senkou_a_d numeric,
    senkou_b_d numeric,
    chikou_d numeric,
    signal_ichimoku_buy_d integer,
    signal_ichimoku_sell_d integer,
    signal_ema_cross_buy_d integer,
    signal_ema_cross_sell_d integer,
    signal_rsi_buy_d integer,
    signal_rsi_sell_d integer,
    signal_macd_buy_d integer,
    signal_macd_sell_d integer,
    signal_ema50_100_buy_d integer,
    signal_ema50_100_sell_d integer,
    atr_52_d numeric,
    renko_52_d text COLLATE pg_catalog."default",
    buy_i_volume bigint,
    buy_n_volume bigint,
    buy_i_value bigint,
    buy_n_value bigint,
    buy_n_count bigint,
    sell_i_volume bigint,
    buy_i_count bigint,
    sell_n_volume bigint,
    sell_i_value bigint,
    sell_n_value bigint,
    sell_n_count bigint,
    sell_i_count bigint,
    name_en text COLLATE pg_catalog."default",
    sector text COLLATE pg_catalog."default",
    sector_code bigint,
    subsector text COLLATE pg_catalog."default",
    panel text COLLATE pg_catalog."default",
    share_number bigint,
    base_vol bigint,
    "instrumentID" text COLLATE pg_catalog."default",
    marketcap bigint,
    marketcap_usd numeric
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.weekly_joined_data
    OWNER to postgres;

-- Table: public.weekly_stock_data

-- DROP TABLE IF EXISTS public.weekly_stock_data;

CREATE TABLE IF NOT EXISTS public.weekly_stock_data
(
    id integer NOT NULL DEFAULT nextval('weekly_stock_data_id_seq'::regclass),
    stock_ticker text COLLATE pg_catalog."default",
    week_start date,
    week_end date,
    open integer,
    high integer,
    low integer,
    close integer,
    final_price integer,
    adjust_open integer,
    adjust_high integer,
    adjust_low integer,
    adjust_close integer,
    adjust_final_price integer,
    volume bigint,
    value bigint,
    name text COLLATE pg_catalog."default",
    market text COLLATE pg_catalog."default",
    adjust_open_usd numeric,
    adjust_high_usd numeric,
    adjust_low_usd numeric,
    adjust_close_usd numeric,
    value_usd numeric,
    dollar_rate numeric,
    CONSTRAINT weekly_stock_data_pkey PRIMARY KEY (id),
    CONSTRAINT weekly_stock_data_stock_ticker_week_end_key UNIQUE (stock_ticker, week_end)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.weekly_stock_data
    OWNER to postgres;