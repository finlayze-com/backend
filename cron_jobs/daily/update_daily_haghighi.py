import requests
import pandas as pd
from datetime import datetime
import psycopg2
import time
import sys
import os
from dotenv import load_dotenv

# خروجی ترمینال UTF-8
sys.stdout.reconfigure(encoding='utf-8')

# بارگذاری env
load_dotenv()

def update_haghighi_data():
    # اتصال امن به دیتابیس
    conn = psycopg2.connect(os.getenv("DB_URL"))
    cursor = conn.cursor()
    conn.rollback()

    symbol_query = '''
        SELECT stock_ticker, "insCode", sector 
        FROM symboldetail
        WHERE panel NOT IN (
            'بازار ابزارهای مشتقه',
            'بازار ابزارهای نوین مالی',
            'بازار ابزارهاي نوين مالي فرابورس',
            'بازار اوراق بدهی'
        )
        AND panel IS NOT NULL
        AND panel NOT LIKE '-%';
    '''

    df_symbols = pd.read_sql(symbol_query, conn)

    symbol_map = {
        row["insCode"]: (row["stock_ticker"], row["sector"])
        for _, row in df_symbols.iterrows()
    }

    def safe_div(a, b):
        try:
            return a / b if a is not None and b not in (None, 0) else None
        except:
            return None

    def get_dollar_rate_for_date(rec_date):
        cursor.execute("""
            SELECT close FROM dollar_data
            WHERE date_miladi <= %s
            ORDER BY date_miladi DESC
            LIMIT 1
        """, (rec_date,))
        result = cursor.fetchone()
        return result[0] if result else None

    for inscode in symbol_map:
        stock_ticker, sector = symbol_map[inscode]
        url = f"https://cdn.tsetmc.com/api/ClientType/GetClientTypeHistory/{inscode}"

        retries = 3
        for attempt in range(retries):
            try:
                response = requests.get(url, timeout=20)
                json_data = response.json()
                break
            except Exception as e:
                print(f"⏳ تلاش {attempt+1} برای {stock_ticker} شکست خورد: {e}")
                time.sleep(2)
        else:
            print(f"❌ شکست نهایی برای {stock_ticker}")
            continue

        if 'clientType' in json_data and isinstance(json_data['clientType'], list):
            for row in json_data['clientType']:
                rec_date = datetime.strptime(str(row['recDate']), '%Y%m%d').date()
                dollar_rate = get_dollar_rate_for_date(rec_date)

                buy_i_value_usd = safe_div(row['buy_I_Value'], dollar_rate)
                buy_n_value_usd = safe_div(row['buy_N_Value'], dollar_rate)
                sell_i_value_usd = safe_div(row['sell_I_Value'], dollar_rate)
                sell_n_value_usd = safe_div(row['sell_N_Value'], dollar_rate)

                try:
                    cursor.execute("""
                        INSERT INTO haghighi (
                            recDate, insCode, buy_I_Volume, buy_N_Volume, buy_I_Value, buy_N_Value,
                            buy_N_Count, sell_I_Volume, buy_I_Count, sell_N_Volume,
                            sell_I_Value, sell_N_Value, sell_N_Count, sell_I_Count,
                            symbol, sector, dollar_rate,
                            buy_i_value_usd, buy_n_value_usd, sell_i_value_usd, sell_n_value_usd
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (symbol, recDate) DO NOTHING;
                    """, (
                        rec_date,
                        inscode,
                        row['buy_I_Volume'],
                        row['buy_N_Volume'],
                        row['buy_I_Value'],
                        row['buy_N_Value'],
                        row['buy_N_Count'],
                        row['sell_I_Volume'],
                        row['buy_I_Count'],
                        row['sell_N_Volume'],
                        row['sell_I_Value'],
                        row['sell_N_Value'],
                        row['sell_N_Count'],
                        row['sell_I_Count'],
                        stock_ticker,
                        sector,
                        dollar_rate,
                        buy_i_value_usd,
                        buy_n_value_usd,
                        sell_i_value_usd,
                        sell_n_value_usd
                    ))
                except Exception as row_err:
                    print(f"⚠️ خطا در ذخیره {stock_ticker} - {rec_date}: {row_err}")

            conn.commit()
            print(f"✅ {stock_ticker} ذخیره شد.")
        else:
            print(f"⚠️ داده‌ای برای {stock_ticker} نیست یا ساختار ناقصه.")

    cursor.close()
    conn.close()

# اجرای تابع اصلی
if __name__ == "__main__":
    update_haghighi_data()
