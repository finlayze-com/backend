# -*- coding: utf-8 -*-
"""
اجرای محاسبه اندیکاتورهای هفتگی برای سهام (saham).
خواندن از weekly_stock_data و درج در weekly_indicators.
"""

from cron_jobs.weekly.common.base_weekly_indicator import build_weekly_indicators_for_table

if __name__ == "__main__":
    print("🔄 Running Weekly Indicator Builder for: saham")
    build_weekly_indicators_for_table(
        source_table="weekly_stock_data",
        dest_table="weekly_indicators",
        insert_mode="upsert",  # یا "replace_all" برای بازسازی کامل
    )
