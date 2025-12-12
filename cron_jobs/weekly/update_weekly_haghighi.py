import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import sys

# برای نمایش درست حروف فارسی در ترمینال
sys.stdout.reconfigure(encoding='utf-8')


def weekly_haghighi_data():
    # ⚠️ اگر روی سرور از DB_URL استفاده می‌کنی، این بخش را مطابق تنظیمات خودت تغییر بده
    db_config = {
        'user': 'postgres',
        'password': 'Afiroozi12',
        'host': 'localhost',
        'port': '5432',
        'database': 'postgres1'
    }

    engine = create_engine(
        f"postgresql://{db_config['user']}:{db_config['password']}@"
        f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
    )
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()

    # ---------- 1) خواندن داده‌های روزانه از haghighi ----------
    df = pd.read_sql("SELECT * FROM haghighi", engine)

    if df.empty:
        print("📭 جدول haghighi خالی است؛ دیتای هفتگی ساخته نشد.")
        cur.close()
        conn.close()
        return

    df['recdate'] = pd.to_datetime(df['recdate'])

    # ---------- 2) گروه‌بندی هفتگی بر اساس symbol و هفته‌های منتهی به جمعه ----------
    grouped = df.groupby(
        ['symbol', pd.Grouper(key='recdate', freq='W-FRI', label='left', closed='left')]
    ).agg({
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
        # ستون‌های دلاری
        'buy_i_value_usd': 'sum',
        'buy_n_value_usd': 'sum',
        'sell_i_value_usd': 'sum',
        'sell_n_value_usd': 'sum',
    }).reset_index()

    if grouped.empty:
        print("📭 بعد از گروه‌بندی، دیتای هفتگی‌ای وجود ندارد.")
        cur.close()
        conn.close()
        return

    grouped.rename(columns={'recdate': 'week_start'}, inplace=True)
    grouped['week_end'] = grouped['week_start'] + pd.Timedelta(days=6)

    # ---------- 3) آخرین هفته‌ی ذخیره‌شده در weekly_haghighi ----------
    cur.execute("SELECT MAX(week_end) FROM weekly_haghighi")
    last_saved = cur.fetchone()[0]
    last_saved = pd.to_datetime(last_saved) if last_saved else pd.Timestamp("1900-01-01")

    # بزرگ‌ترین week_end در دیتای فعلی (هفته‌ی جاری / آخرین هفته)
    max_week_end = grouped['week_end'].max()

    # اگر آخرین هفته‌ای که داریم <= آخرین هفته ذخیره‌شده باشد، یعنی هیچ هفته‌ی جدیدی نداریم
    if max_week_end < last_saved:
        print("📭 هیچ هفته‌ی جدیدی نسبت به آخرین ذخیره وجود ندارد.")
        cur.close()
        conn.close()
        return

    # ---------- 4) تقسیم دیتای هفتگی به «هفته‌های قدیمی» و «فقط آخرین هفته» ----------

    # هفته‌های قدیمی‌تر از max_week_end و بعد از last_saved:
    # این‌ها فقط یک‌بار Insert می‌شن و بعدش دیگر دست نمی‌خورند.
    old_weeks_df = grouped[
        (grouped['week_end'] > last_saved) &
        (grouped['week_end'] < max_week_end)
    ].copy()

    # فقط آخرین هفته (week_end برابر با max_week_end)
    # این هفته هر بار که اسکریپت اجرا شود، دوباره محاسبه و UPSERT می‌شود.
    last_week_df = grouped[grouped['week_end'] == max_week_end].copy()

    if old_weeks_df.empty and last_week_df.empty:
        print("📭 هفته‌ای برای ذخیره یا به‌روزرسانی یافت نشد.")
        cur.close()
        conn.close()
        return

    # ستون‌هایی که باید به دیتابیس بفرستیم
    cols = [
        'symbol', 'week_start', 'week_end',
        'buy_i_volume', 'buy_n_volume',
        'buy_i_value', 'buy_n_value', 'buy_n_count',
        'sell_i_volume', 'buy_i_count', 'sell_n_volume',
        'sell_i_value', 'sell_n_value', 'sell_n_count', 'sell_i_count',
        'buy_i_value_usd', 'buy_n_value_usd', 'sell_i_value_usd', 'sell_n_value_usd'
    ]

    # ---------- 5) INSERT برای هفته‌های قدیمی (فقط یک‌بار، بدون آپدیت) ----------
    if not old_weeks_df.empty:
        insert_query_old = """
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

        with conn.cursor() as cur_old:
            cur_old.executemany(
                insert_query_old,
                old_weeks_df[cols].values.tolist()
            )
        conn.commit()
        print(f"✅ {len(old_weeks_df)} رکورد هفتگی (هفته‌های کامل قبلی) ذخیره شد.")

    # ---------- 6) UPSERT برای فقط آخرین هفته (همیشه آپدیت شود) ----------
    if not last_week_df.empty:
        insert_query_last = """
        INSERT INTO weekly_haghighi (
            symbol, week_start, week_end,
            buy_i_volume, buy_n_volume,
            buy_i_value, buy_n_value, buy_n_count,
            sell_i_volume, buy_i_count, sell_n_volume,
            sell_i_value, sell_n_value, sell_n_count, sell_i_count,
            buy_i_value_usd, buy_n_value_usd, sell_i_value_usd, sell_n_value_usd
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, week_end) DO UPDATE SET
            buy_i_volume      = EXCLUDED.buy_i_volume,
            buy_n_volume      = EXCLUDED.buy_n_volume,
            buy_i_value       = EXCLUDED.buy_i_value,
            buy_n_value       = EXCLUDED.buy_n_value,
            buy_n_count       = EXCLUDED.buy_n_count,
            sell_i_volume     = EXCLUDED.sell_i_volume,
            buy_i_count       = EXCLUDED.buy_i_count,
            sell_n_volume     = EXCLUDED.sell_n_volume,
            sell_i_value      = EXCLUDED.sell_i_value,
            sell_n_value      = EXCLUDED.sell_n_value,
            sell_n_count      = EXCLUDED.sell_n_count,
            sell_i_count      = EXCLUDED.sell_i_count,
            buy_i_value_usd   = EXCLUDED.buy_i_value_usd,
            buy_n_value_usd   = EXCLUDED.buy_n_value_usd,
            sell_i_value_usd  = EXCLUDED.sell_i_value_usd,
            sell_n_value_usd  = EXCLUDED.sell_n_value_usd;
        """

        with conn.cursor() as cur_last:
            cur_last.executemany(
                insert_query_last,
                last_week_df[cols].values.tolist()
            )
        conn.commit()
        print(f"🔄 {len(last_week_df)} رکورد مربوط به آخرین هفته ذخیره/آپدیت شد.")

    cur.close()
    conn.close()


if __name__ == "__main__":
    weekly_haghighi_data()
