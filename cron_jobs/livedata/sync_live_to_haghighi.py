import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import psycopg2

# اتصال به دیتابیس
DB_URI = "postgresql://postgres:Afiroozi12@localhost:5432/postgres1"
engine = create_engine(DB_URI)
conn = psycopg2.connect(host="localhost", dbname="postgres1", user="postgres", password="Afiroozi12")
cur = conn.cursor()

# 1. خواندن داده‌های live_market_data
live_df = pd.read_sql("SELECT * FROM live_market_data", engine)
live_df['updated_at'] = pd.to_datetime(live_df['updated_at'])
live_df = live_df.sort_values('updated_at').drop_duplicates('Ticker', keep='last')
live_df['recdate'] = live_df['updated_at'].dt.date

# 2. گرفتن insCode و sector از symbolDetail
symbol_df = pd.read_sql('SELECT stock_ticker, "insCode", sector FROM symbolDetail', engine)
live_df = live_df.merge(symbol_df, left_on='Ticker', right_on='stock_ticker', how='left')

# 3. گرفتن نرخ دلار
dollar_df = pd.read_sql("SELECT date_miladi, close FROM dollar_data", engine)
dollar_df['date_miladi'] = pd.to_datetime(dollar_df['date_miladi'])
dollar_df = dollar_df.set_index('date_miladi').sort_index()

def get_nearest_dollar(date):
    date = pd.to_datetime(date)
    before = dollar_df.loc[:date]
    return before.iloc[-1]['close'] if not before.empty else None

live_df['dollar_rate'] = live_df['recdate'].apply(get_nearest_dollar)

# 4. محاسبه ستون‌های دلاری
live_df['buy_i_value'] = live_df['Vol_Buy_I'] * live_df['Close']
live_df['buy_n_value'] = live_df['Vol_Buy_R'] * live_df['Close']
live_df['sell_i_value'] = live_df['Vol_Sell_I'] * live_df['Close']
live_df['sell_n_value'] = live_df['Vol_Sell_R'] * live_df['Close']

live_df['buy_i_value_usd'] = live_df['buy_i_value'] / live_df['dollar_rate']
live_df['buy_n_value_usd'] = live_df['buy_n_value'] / live_df['dollar_rate']
live_df['sell_i_value_usd'] = live_df['sell_i_value'] / live_df['dollar_rate']
live_df['sell_n_value_usd'] = live_df['sell_n_value'] / live_df['dollar_rate']

# 5. حذف رکوردهای قبلی
cur.execute("DELETE FROM haghighi WHERE is_temp = TRUE;")

# 6. آماده‌سازی داده برای درج
records = list(zip(
    live_df['recdate'], live_df['insCode'],
    live_df['Vol_Buy_I'], live_df['Vol_Buy_R'],
    live_df['buy_i_value'], live_df['buy_n_value'],
    live_df['No_Buy_R'], live_df['Vol_Sell_I'],
    live_df['No_Buy_I'], live_df['Vol_Sell_R'],
    live_df['sell_i_value'], live_df['sell_n_value'],
    live_df['No_Sell_R'], live_df['No_Sell_I'],
    live_df['Ticker'], live_df['dollar_rate'],
    live_df['buy_i_value_usd'], live_df['buy_n_value_usd'],
    live_df['sell_i_value_usd'], live_df['sell_n_value_usd'],
    live_df['sector'],
    [True] * len(live_df)
))

print("MAX buy_i_value:", live_df["buy_i_value"].max())
print("MAX buy_n_value:", live_df["buy_n_value"].max())
print("MAX sell_i_value:", live_df["sell_i_value"].max())
print("MAX sell_n_value:", live_df["sell_n_value"].max())

# 7. درج در جدول
insert_query = """
INSERT INTO haghighi (
    recdate, insCode,
    buy_i_volume, buy_n_volume,
    buy_i_value, buy_n_value,
    buy_n_count, sell_i_volume,
    buy_i_count, sell_n_volume,
    sell_i_value, sell_n_value,
    sell_n_count, sell_i_count,
    symbol, dollar_rate,
    buy_i_value_usd, buy_n_value_usd,
    sell_i_value_usd, sell_n_value_usd,
    sector, is_temp
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (symbol, recdate) DO UPDATE SET
    buy_i_volume = EXCLUDED.buy_i_volume,
    buy_n_volume = EXCLUDED.buy_n_volume,
    buy_i_value = EXCLUDED.buy_i_value,
    buy_n_value = EXCLUDED.buy_n_value,
    buy_n_count = EXCLUDED.buy_n_count,
    sell_i_volume = EXCLUDED.sell_i_volume,
    buy_i_count = EXCLUDED.buy_i_count,
    sell_n_volume = EXCLUDED.sell_n_volume,
    sell_i_value = EXCLUDED.sell_i_value,
    sell_n_value = EXCLUDED.sell_n_value,
    sell_n_count = EXCLUDED.sell_n_count,
    sell_i_count = EXCLUDED.sell_i_count,
    dollar_rate = EXCLUDED.dollar_rate,
    buy_i_value_usd = EXCLUDED.buy_i_value_usd,
    buy_n_value_usd = EXCLUDED.buy_n_value_usd,
    sell_i_value_usd = EXCLUDED.sell_i_value_usd,
    sell_n_value_usd = EXCLUDED.sell_n_value_usd,
    sector = EXCLUDED.sector,
    is_temp = EXCLUDED.is_temp;
"""

cur.executemany(insert_query, records)
conn.commit()
cur.close()
conn.close()
print("✅ داده‌های حقیقی با موفقیت ذخیره شدند.")
