from cron_jobs.weekly.common.base_weekly_updater import build_weekly_from_daily

if __name__ == "__main__":
    build_weekly_from_daily(
        src_table="daily_stock_data",
        dst_table="weekly_stock_data",
        date_col="date_miladi",
        symbol_col="stock_ticker",
        extra_identity_cols=["name", "market"],    # مطابق اسکریپت قبلی‌ات
        conflict_on=("stock_ticker", "week_end")
    )
