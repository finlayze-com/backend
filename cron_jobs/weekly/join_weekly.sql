DROP TABLE IF EXISTS weekly_joined_data;

CREATE TABLE weekly_joined_data AS
SELECT
    -- 🟢 weekly_stock_data: همه ستون‌ها
    wsd.*,

    -- 🔵 weekly_indicators (بدون stock_ticker و week_end)
    wi.ema_20, wi.ema_50, wi.ema_100,
    wi.rsi, wi.macd, wi.macd_signal, wi.macd_hist,
    wi.tenkan, wi.kijun, wi.senkou_a, wi.senkou_b, wi.chikou,
    wi.signal_ichimoku_buy, wi.signal_ichimoku_sell,
    wi.signal_ema_cross_buy, wi.signal_ema_cross_sell,
    wi.signal_rsi_buy, wi.signal_rsi_sell,
    wi.signal_macd_buy, wi.signal_macd_sell,
    wi.signal_ema50_100_buy, wi.signal_ema50_100_sell,
    wi.atr_52, wi.renko_52,
    wi.ema_20_d, wi.ema_50_d, wi.ema_100_d,
    wi.rsi_d, wi.macd_d, wi.macd_signal_d, wi.macd_hist_d,
    wi.tenkan_d, wi.kijun_d, wi.senkou_a_d, wi.senkou_b_d, wi.chikou_d,
    wi.signal_ichimoku_buy_d, wi.signal_ichimoku_sell_d,
    wi.signal_ema_cross_buy_d, wi.signal_ema_cross_sell_d,
    wi.signal_rsi_buy_d, wi.signal_rsi_sell_d,
    wi.signal_macd_buy_d, wi.signal_macd_sell_d,
    wi.signal_ema50_100_buy_d, wi.signal_ema50_100_sell_d,
    wi.atr_52_d, wi.renko_52_d,

    -- 🟠 weekly_haghighi (بدون symbol, week_start, week_end)
    wh.buy_i_volume, wh.buy_n_volume,
    wh.buy_i_value, wh.buy_n_value, wh.buy_n_count,
    wh.sell_i_volume, wh.buy_i_count, wh.sell_n_volume,
    wh.sell_i_value, wh.sell_n_value, wh.sell_n_count, wh.sell_i_count,

    -- 🟣 symboldetail (بدون stock_ticker, name, market)
    sd.name_en, sd.sector, sd.sector_code, sd.subsector,
    sd.panel, sd.share_number, sd.base_vol, sd."instrumentID",

    -- 💰 محاسبات بازار هفتگی
    (wsd.adjust_close * sd.share_number) AS marketcap,
    (wsd.adjust_close * sd.share_number) / wsd.dollar_rate AS marketcap_usd

FROM
    weekly_stock_data wsd
LEFT JOIN
    weekly_indicators wi
    ON wsd.stock_ticker = wi.stock_ticker AND wsd.week_end = wi.week_end
LEFT JOIN
    weekly_haghighi wh
    ON wsd.stock_ticker = wh.symbol AND wsd.week_end = wh.week_end
LEFT JOIN
    symboldetail sd
    ON wsd.stock_ticker = sd.stock_ticker;
