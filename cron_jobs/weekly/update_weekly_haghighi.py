import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import sys
sys.stdout.reconfigure(encoding='utf-8')

def weekly_haghighi_data():
    db_config = {
        'user': 'postgres',
        'password': 'Afiroozi12',
        'host': 'localhost',
        'port': '5432',
        'database': 'postgres1'
    }

    engine = create_engine(f"postgresql://{db_config['user']}:{db_config['password']}@"
                           f"{db_config['host']}:{db_config['port']}/{db_config['database']}")
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()

    # گرفتن داده‌ها
    df = pd.read_sql("SELECT * FROM haghighi", engine)
    df['recdate'] = pd.to_datetime(df['recdate'])

    # گروه‌بندی هفتگی
    grouped = df.groupby(['symbol', pd.Grouper(key='recdate', freq='W-FRI', label='left', closed='left')]).agg({
        'buy_i_volume': 'sum',
        'buy_n_volume': 'sum',
        'buy_i_value': 'sum',
        'buy_n_value': 'sum',
        'buy_n_count': 'sum',
        'sell_i_volume': 'sum',
        'buy_i_count': 'sum',
        'sell_n_volume': 'sum',
        'sell_i_value': 'sum',
        'sell_n_value': 'sum',
        'sell_n_count': 'sum',
        'sell_i_count': 'sum',
        # 👇 این‌ها را اضافه کن
        'buy_i_value_usd': 'sum',
        'buy_n_value_usd': 'sum',
        'sell_i_value_usd': 'sum',
        'sell_n_value_usd': 'sum',
    }).reset_index()

    grouped.rename(columns={'recdate': 'week_start'}, inplace=True)
    grouped['week_end'] = grouped['week_start'] + pd.Timedelta(days=6)

    # بررسی آخرین هفته ذخیره شده
    cur.execute("SELECT MAX(week_end) FROM weekly_haghighi")
    last_saved = cur.fetchone()[0]
    last_saved = pd.to_datetime(last_saved) if last_saved else pd.Timestamp("1900-01-01")

    today = pd.Timestamp.today().normalize()
    grouped = grouped[(grouped['week_end'] > last_saved) & (grouped['week_end'] < today)]

    if grouped.empty:
        print("📭 هفته جدیدی برای ذخیره نیست.")
    else:
        insert_query = """
        INSERT INTO weekly_haghighi (
            symbol, week_start, week_end,
            buy_i_volume, buy_n_volume,
            buy_i_value, buy_n_value, buy_n_count,
            sell_i_volume, buy_i_count, sell_n_volume,
            sell_i_value, sell_n_value, sell_n_count, sell_i_count,
            buy_i_value_usd, buy_n_value_usd, sell_i_value_usd, sell_n_value_usd
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, week_end) DO NOTHING;
        """

        with conn.cursor() as cur:
            cur.executemany(insert_query, grouped[[
                'symbol', 'week_start', 'week_end',
                'buy_i_volume', 'buy_n_volume',
                'buy_i_value', 'buy_n_value', 'buy_n_count',
                'sell_i_volume', 'buy_i_count', 'sell_n_volume',
                'sell_i_value', 'sell_n_value', 'sell_n_count', 'sell_i_count',
                'buy_i_value_usd', 'buy_n_value_usd', 'sell_i_value_usd', 'sell_n_value_usd'
            ]].values.tolist())
            conn.commit()

        print(f"✅ {len(grouped)} رکورد هفتگی ذخیره شد.")

    cur.close()
    conn.close()

# ✅ فقط این قسمت جدید اضافه شده:
if __name__ == "__main__":
    weekly_haghighi_data()