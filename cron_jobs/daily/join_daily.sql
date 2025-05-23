DROP TABLE IF EXISTS daily_joined_data;

CREATE TABLE daily_joined_data AS
SELECT
    -- 🟢 daily_stock_data: همه ستون‌ها
    dsd.*,

    -- 🔵 daily_indicators (بدون stock_ticker و date_miladi)
    di.ema_20, di.ema_50, di.ema_100,
    di.rsi, di.macd, di.macd_signal, di.macd_hist,
    di.tenkan, di.kijun, di.senkou_a, di.senkou_b, di.chikou,
    di.signal_ichimoku_buy, di.signal_ichimoku_sell,
    di.signal_ema_cross_buy, di.signal_ema_cross_sell,
    di.signal_rsi_buy, di.signal_rsi_sell,
    di.signal_macd_buy, di.signal_macd_sell,
    di.signal_ema50_100_buy, di.signal_ema50_100_sell,
    di.atr_22, di.renko_22,
	    -- 💵 سیگنال‌های دلاری
    di.signal_ichimoku_buy_d   AS signal_ichimoku_buy_usd,
    di.signal_ichimoku_sell_d  AS signal_ichimoku_sell_usd,
    di.signal_ema_cross_buy_d  AS signal_ema_cross_buy_usd,
    di.signal_ema_cross_sell_d AS signal_ema_cross_sell_usd,
    di.signal_rsi_buy_d        AS signal_rsi_buy_usd,
    di.signal_rsi_sell_d       AS signal_rsi_sell_usd,
    di.signal_macd_buy_d       AS signal_macd_buy_usd,
    di.signal_macd_sell_d      AS signal_macd_sell_usd,
    di.signal_ema50_100_buy_d  AS signal_ema50_100_buy_usd,
    di.signal_ema50_100_sell_d AS signal_ema50_100_sell_usd,
    di.renko_22_d              AS renko_22_usd,


    -- 🟠 haghighi (بدون recDate, symbol, sector, dollar_rate)
    h.insCode,
    h.buy_I_Volume, h.buy_N_Volume, h.buy_I_Value, h.buy_N_Value,
    h.buy_N_Count, h.sell_I_Volume, h.buy_I_Count, h.sell_N_Volume,
    h.sell_I_Value, h.sell_N_Value, h.sell_N_Count, h.sell_I_Count,
    h.buy_i_value_usd, h.buy_n_value_usd, h.sell_i_value_usd, h.sell_n_value_usd,

    -- 🟣 symboldetail (بدون stock_ticker, name, sector, market)
    sd.name_en,sd.sector, sd.sector_code, sd.subsector, sd.market AS market2,
    sd.panel, sd.share_number, sd.base_vol, sd."instrumentID",


    -- 💰 محاسبات بازار
    (dsd.adjust_close * sd.share_number) AS marketcap,
    (dsd.adjust_close * sd.share_number) / dsd.dollar_rate AS marketcap_usd
	
FROM
    daily_stock_data dsd
LEFT JOIN
    daily_indicators di
    ON dsd.stock_ticker = di.stock_ticker AND dsd.date_miladi = di.date_miladi
LEFT JOIN
    haghighi h
    ON dsd.stock_ticker = h.symbol AND dsd.date_miladi = h.recDate
LEFT JOIN
    symboldetail sd
    ON dsd.stock_ticker = sd.stock_ticker;