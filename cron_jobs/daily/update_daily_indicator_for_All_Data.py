import pandas as pd
import numpy as np
import talib
import psycopg2
from sqlalchemy import create_engine
import sys
sys.stdout.reconfigure(encoding='utf-8')

def generate_renko_signal_direction_v2(prices, box_size):
    renko = []
    last_renko_price = None
    current_trend = None

    for price in prices:
        if pd.isna(price) or pd.isna(box_size):
            renko.append(None)
            continue

        if last_renko_price is None:
            last_renko_price = price
            renko.append(None)
            continue

        change = price - last_renko_price

        if pd.isna(change) or box_size == 0:
            renko.append(None)
            continue

        try:
            brick_count = int(change / box_size)
        except (ValueError, ZeroDivisionError):
            renko.append(None)
            continue

        if abs(brick_count) >= 1:
            direction = 'صعودی' if brick_count > 0 else 'نزولی'

            if current_trend is None:
                renko.append(direction)
            elif current_trend == 'صعودی' and direction == 'نزولی':
                renko.append('سیگنال فروش')
            elif current_trend == 'نزولی' and direction == 'صعودی':
                renko.append('سیگنال خرید')
            else:
                renko.append(direction)

            last_renko_price += box_size * brick_count
            current_trend = direction
        else:
            renko.append(current_trend)

    return renko

def build_daily_indicators():
    db_config = {
        'host': 'localhost',
        'dbname': 'postgres1',
        'user': 'postgres',
        'password': 'Afiroozi12'
    }

    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    engine = create_engine(f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:5432/{db_config['dbname']}")

    df = pd.read_sql("SELECT * FROM daily_stock_data", engine)
    df['date_miladi'] = pd.to_datetime(df['date_miladi'])

    all_records = []

    for symbol in df['stock_ticker'].unique():
        data = df[df['stock_ticker'] == symbol].copy()
        data.sort_values('date_miladi', inplace=True)

        close = pd.to_numeric(data['adjust_close'], errors='coerce')
        high = pd.to_numeric(data['adjust_high'], errors='coerce')
        low = pd.to_numeric(data['adjust_low'], errors='coerce')

        data['ema_20'] = talib.EMA(close, 20)
        data['ema_50'] = talib.EMA(close, 50)
        data['ema_100'] = talib.EMA(close, 100)
        data['rsi'] = talib.RSI(close, 14)
        macd, macd_signal, macd_hist = talib.MACD(close)
        data['macd'], data['macd_signal'], data['macd_hist'] = macd, macd_signal, macd_hist

        data['tenkan'] = (high.rolling(9).max() + low.rolling(9).min()) / 2
        data['kijun'] = (high.rolling(26).max() + low.rolling(26).min()) / 2
        data['senkou_a'] = ((data['tenkan'] + data['kijun']) / 2).shift(26)
        data['senkou_b'] = ((high.rolling(52).max() + low.rolling(52).min()) / 2).shift(26)
        data['chikou'] = close.shift(-26)

        data['atr_22'] = talib.ATR(high, low, close, timeperiod=22)

        # اطمینان از مقدار معتبر برای box_size از ATR
        box_size = data['atr_22'].dropna().iloc[-1] if not data['atr_22'].dropna().empty else 100

        data['renko_22'] = generate_renko_signal_direction_v2(close, box_size)

        data['signal_ichimoku_buy'] = np.where((data['tenkan'] > data['kijun']) & (data['tenkan'].shift(1) < data['kijun'].shift(1)), 1, 0)
        data['signal_ichimoku_sell'] = np.where((data['tenkan'] < data['kijun']) & (data['tenkan'].shift(1) > data['kijun'].shift(1)), 1, 0)
        data['signal_ema_cross_buy'] = np.where((data['ema_20'].shift(1) < data['ema_50'].shift(1)) & (data['ema_20'] > data['ema_50']), 1, 0)
        data['signal_ema_cross_sell'] = np.where((data['ema_20'].shift(1) > data['ema_50'].shift(1)) & (data['ema_20'] < data['ema_50']), 1, 0)
        data['signal_rsi_buy'] = np.where((data['rsi'].shift(1) < 30) & (data['rsi'] > 30), 1, 0)
        data['signal_rsi_sell'] = np.where((data['rsi'].shift(1) > 70) & (data['rsi'] < 70), 1, 0)
        data['signal_macd_buy'] = np.where((data['macd'].shift(1) < data['macd_signal'].shift(1)) & (data['macd'] > data['macd_signal']), 1, 0)
        data['signal_macd_sell'] = np.where((data['macd'].shift(1) > data['macd_signal'].shift(1)) & (data['macd'] < data['macd_signal']), 1, 0)
        data['signal_ema50_100_buy'] = np.where((data['ema_50'].shift(1) < data['ema_100'].shift(1)) &
                                                (data['ema_50'] > data['ema_100']) &
                                                (data['ema_20'] > data['ema_100']), 1, 0)
        data['signal_ema50_100_sell'] = np.where((data['ema_50'].shift(1) > data['ema_100'].shift(1)) &
                                                 (data['ema_50'] < data['ema_100']) &
                                                 (data['ema_20'] < data['ema_100']), 1, 0)
        # === نسخه دلاری اندیکاتورها ===
        close_d = pd.to_numeric(data['adjust_close_usd'], errors='coerce')
        high_d = pd.to_numeric(data['adjust_high'] / data['dollar_rate'], errors='coerce')
        low_d = pd.to_numeric(data['adjust_low'] / data['dollar_rate'], errors='coerce')

        data['ema_20_d'] = talib.EMA(close_d, 20)
        data['ema_50_d'] = talib.EMA(close_d, 50)
        data['ema_100_d'] = talib.EMA(close_d, 100)
        data['rsi_d'] = talib.RSI(close_d, 14)
        macd_d, macd_signal_d, macd_hist_d = talib.MACD(close_d)
        data['macd_d'], data['macd_signal_d'], data['macd_hist_d'] = macd_d, macd_signal_d, macd_hist_d

        data['tenkan_d'] = (high_d.rolling(9).max() + low_d.rolling(9).min()) / 2
        data['kijun_d'] = (high_d.rolling(26).max() + low_d.rolling(26).min()) / 2
        data['senkou_a_d'] = ((data['tenkan_d'] + data['kijun_d']) / 2).shift(26)
        data['senkou_b_d'] = ((high_d.rolling(52).max() + low_d.rolling(52).min()) / 2).shift(26)
        data['chikou_d'] = close_d.shift(-26)
        data['atr_22_d'] = talib.ATR(high_d, low_d, close_d, timeperiod=22)

        box_size_d = data['atr_22_d'].dropna().iloc[-1] if not data['atr_22_d'].dropna().empty else 100
        data['renko_22_d'] = generate_renko_signal_direction_v2(close_d, box_size_d)

        data['signal_ichimoku_buy_d'] = np.where(
            (data['tenkan_d'] > data['kijun_d']) & (data['tenkan_d'].shift(1) < data['kijun_d'].shift(1)), 1, 0)
        data['signal_ichimoku_sell_d'] = np.where(
            (data['tenkan_d'] < data['kijun_d']) & (data['tenkan_d'].shift(1) > data['kijun_d'].shift(1)), 1, 0)
        data['signal_ema_cross_buy_d'] = np.where(
            (data['ema_20_d'].shift(1) < data['ema_50_d'].shift(1)) & (data['ema_20_d'] > data['ema_50_d']), 1, 0)
        data['signal_ema_cross_sell_d'] = np.where(
            (data['ema_20_d'].shift(1) > data['ema_50_d'].shift(1)) & (data['ema_20_d'] < data['ema_50_d']), 1, 0)
        data['signal_rsi_buy_d'] = np.where((data['rsi_d'].shift(1) < 30) & (data['rsi_d'] > 30), 1, 0)
        data['signal_rsi_sell_d'] = np.where((data['rsi_d'].shift(1) > 70) & (data['rsi_d'] < 70), 1, 0)
        data['signal_macd_buy_d'] = np.where(
            (data['macd_d'].shift(1) < data['macd_signal_d'].shift(1)) & (data['macd_d'] > data['macd_signal_d']), 1, 0)
        data['signal_macd_sell_d'] = np.where(
            (data['macd_d'].shift(1) > data['macd_signal_d'].shift(1)) & (data['macd_d'] < data['macd_signal_d']), 1, 0)
        data['signal_ema50_100_buy_d'] = np.where((data['ema_50_d'].shift(1) < data['ema_100_d'].shift(1)) &
                                                  (data['ema_50_d'] > data['ema_100_d']) &
                                                  (data['ema_20_d'] > data['ema_100_d']), 1, 0)
        data['signal_ema50_100_sell_d'] = np.where((data['ema_50_d'].shift(1) > data['ema_100_d'].shift(1)) &
                                                   (data['ema_50_d'] < data['ema_100_d']) &
                                                   (data['ema_20_d'] < data['ema_100_d']), 1, 0)

        all_records.extend(data[[
            'stock_ticker', 'date_miladi',
            'ema_20', 'ema_50', 'ema_100',
            'rsi', 'macd', 'macd_signal', 'macd_hist',
            'tenkan', 'kijun', 'senkou_a', 'senkou_b', 'chikou',
            'signal_ichimoku_buy', 'signal_ichimoku_sell',
            'signal_ema_cross_buy', 'signal_ema_cross_sell',
            'signal_rsi_buy', 'signal_rsi_sell',
            'signal_macd_buy', 'signal_macd_sell',
            'signal_ema50_100_buy', 'signal_ema50_100_sell',
            'atr_22', 'renko_22',
            'ema_20_d', 'ema_50_d', 'ema_100_d',
            'rsi_d', 'macd_d', 'macd_signal_d', 'macd_hist_d',
            'tenkan_d', 'kijun_d', 'senkou_a_d', 'senkou_b_d', 'chikou_d',
            'signal_ichimoku_buy_d', 'signal_ichimoku_sell_d',
            'signal_ema_cross_buy_d', 'signal_ema_cross_sell_d',
            'signal_rsi_buy_d', 'signal_rsi_sell_d',
            'signal_macd_buy_d', 'signal_macd_sell_d',
            'signal_ema50_100_buy_d', 'signal_ema50_100_sell_d',
            'atr_22_d', 'renko_22_d'
        ]].values.tolist())

    cur.execute("TRUNCATE TABLE daily_indicators")
    cur.executemany("""
        INSERT INTO daily_indicators (
            stock_ticker, date_miladi,
            ema_20, ema_50, ema_100,
            rsi, macd, macd_signal, macd_hist,
            tenkan, kijun, senkou_a, senkou_b, chikou,
            signal_ichimoku_buy, signal_ichimoku_sell,
            signal_ema_cross_buy, signal_ema_cross_sell,
            signal_rsi_buy, signal_rsi_sell,
            signal_macd_buy, signal_macd_sell,
            signal_ema50_100_buy, signal_ema50_100_sell,
            atr_22, renko_22,ema_20_d, ema_50_d, ema_100_d,
            rsi_d, macd_d, macd_signal_d, macd_hist_d,
            tenkan_d, kijun_d, senkou_a_d, senkou_b_d, chikou_d,
            signal_ichimoku_buy_d, signal_ichimoku_sell_d,
            signal_ema_cross_buy_d, signal_ema_cross_sell_d,
            signal_rsi_buy_d, signal_rsi_sell_d,
            signal_macd_buy_d, signal_macd_sell_d,
            signal_ema50_100_buy_d, signal_ema50_100_sell_d,
            atr_22_d, renko_22_d
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                  %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                  %s, %s)
    """, all_records)

    conn.commit()
    print(f"✅ {len(all_records)} ردیف اندیکاتور با ATR و Renko ذخیره شد.")
    cur.close()
    conn.close()

# اجرا
# ✅ فقط این قسمت جدید اضافه شده:
if __name__ == "__main__":
    build_daily_indicators()
