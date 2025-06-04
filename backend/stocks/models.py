from typing import List, Optional

from sqlalchemy import BigInteger, Boolean, CHAR, Column, Date, DateTime, Double, Enum, ForeignKeyConstraint, Index, Integer, JSON, Numeric, PrimaryKeyConstraint, SmallInteger, String, Table, Text, UniqueConstraint, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
import datetime
import decimal
# backend/stocks/models.py
from backend.db.connection import Base




class DailyIndicators(Base):
    __tablename__ = 'daily_indicators'
    __table_args__ = (
        PrimaryKeyConstraint('stock_ticker', 'date_miladi', name='daily_indicators_pkey'),
    )

    stock_ticker: Mapped[str] = mapped_column(Text, primary_key=True)
    date_miladi: Mapped[datetime.date] = mapped_column(Date, primary_key=True)
    ema_20: Mapped[Optional[float]] = mapped_column(Double(53))
    ema_50: Mapped[Optional[float]] = mapped_column(Double(53))
    ema_100: Mapped[Optional[float]] = mapped_column(Double(53))
    rsi: Mapped[Optional[float]] = mapped_column(Double(53))
    macd: Mapped[Optional[float]] = mapped_column(Double(53))
    macd_signal: Mapped[Optional[float]] = mapped_column(Double(53))
    macd_hist: Mapped[Optional[float]] = mapped_column(Double(53))
    tenkan: Mapped[Optional[float]] = mapped_column(Double(53))
    kijun: Mapped[Optional[float]] = mapped_column(Double(53))
    senkou_a: Mapped[Optional[float]] = mapped_column(Double(53))
    senkou_b: Mapped[Optional[float]] = mapped_column(Double(53))
    chikou: Mapped[Optional[float]] = mapped_column(Double(53))
    signal_ichimoku_buy: Mapped[Optional[int]] = mapped_column(SmallInteger)
    signal_ichimoku_sell: Mapped[Optional[int]] = mapped_column(SmallInteger)
    signal_ema_cross_buy: Mapped[Optional[int]] = mapped_column(SmallInteger)
    signal_ema_cross_sell: Mapped[Optional[int]] = mapped_column(SmallInteger)
    signal_rsi_buy: Mapped[Optional[int]] = mapped_column(SmallInteger)
    signal_rsi_sell: Mapped[Optional[int]] = mapped_column(SmallInteger)
    signal_macd_buy: Mapped[Optional[int]] = mapped_column(SmallInteger)
    signal_macd_sell: Mapped[Optional[int]] = mapped_column(SmallInteger)
    signal_ema50_100_buy: Mapped[Optional[int]] = mapped_column(SmallInteger)
    signal_ema50_100_sell: Mapped[Optional[int]] = mapped_column(SmallInteger)
    atr_22: Mapped[Optional[float]] = mapped_column(Double(53))
    renko_22: Mapped[Optional[str]] = mapped_column(Text)
    ema_20_d: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    ema_50_d: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    ema_100_d: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    rsi_d: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    macd_d: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    macd_signal_d: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    macd_hist_d: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    tenkan_d: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    kijun_d: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    senkou_a_d: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    senkou_b_d: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    chikou_d: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    signal_ichimoku_buy_d: Mapped[Optional[int]] = mapped_column(Integer)
    signal_ichimoku_sell_d: Mapped[Optional[int]] = mapped_column(Integer)
    signal_ema_cross_buy_d: Mapped[Optional[int]] = mapped_column(Integer)
    signal_ema_cross_sell_d: Mapped[Optional[int]] = mapped_column(Integer)
    signal_rsi_buy_d: Mapped[Optional[int]] = mapped_column(Integer)
    signal_rsi_sell_d: Mapped[Optional[int]] = mapped_column(Integer)
    signal_macd_buy_d: Mapped[Optional[int]] = mapped_column(Integer)
    signal_macd_sell_d: Mapped[Optional[int]] = mapped_column(Integer)
    signal_ema50_100_buy_d: Mapped[Optional[int]] = mapped_column(Integer)
    signal_ema50_100_sell_d: Mapped[Optional[int]] = mapped_column(Integer)
    atr_22_d: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    renko_22_d: Mapped[Optional[str]] = mapped_column(Text)


t_daily_joined_data = Table(
    'daily_joined_data', Base.metadata,
    Column('id', Integer),
    Column('stock_ticker', Text),
    Column('j_date', CHAR(10)),
    Column('date_miladi', Date),
    Column('weekday', Text),
    Column('open', Integer),
    Column('high', Integer),
    Column('low', Integer),
    Column('close', Integer),
    Column('final_price', Integer),
    Column('volume', BigInteger),
    Column('value', BigInteger),
    Column('trade_count', Integer),
    Column('name', Text),
    Column('market', Text),
    Column('adjust_open', Integer),
    Column('adjust_high', Integer),
    Column('adjust_low', Integer),
    Column('adjust_close', Integer),
    Column('adjust_final_price', Integer),
    Column('adjust_open_usd', Double(53)),
    Column('adjust_high_usd', Double(53)),
    Column('adjust_low_usd', Double(53)),
    Column('adjust_close_usd', Double(53)),
    Column('value_usd', Double(53)),
    Column('dollar_rate', Double(53)),
    Column('is_temp', Boolean),
    Column('ema_20', Double(53)),
    Column('ema_50', Double(53)),
    Column('ema_100', Double(53)),
    Column('rsi', Double(53)),
    Column('macd', Double(53)),
    Column('macd_signal', Double(53)),
    Column('macd_hist', Double(53)),
    Column('tenkan', Double(53)),
    Column('kijun', Double(53)),
    Column('senkou_a', Double(53)),
    Column('senkou_b', Double(53)),
    Column('chikou', Double(53)),
    Column('signal_ichimoku_buy', SmallInteger),
    Column('signal_ichimoku_sell', SmallInteger),
    Column('signal_ema_cross_buy', SmallInteger),
    Column('signal_ema_cross_sell', SmallInteger),
    Column('signal_rsi_buy', SmallInteger),
    Column('signal_rsi_sell', SmallInteger),
    Column('signal_macd_buy', SmallInteger),
    Column('signal_macd_sell', SmallInteger),
    Column('signal_ema50_100_buy', SmallInteger),
    Column('signal_ema50_100_sell', SmallInteger),
    Column('atr_22', Double(53)),
    Column('renko_22', Text),
    Column('signal_ichimoku_buy_usd', Integer),
    Column('signal_ichimoku_sell_usd', Integer),
    Column('signal_ema_cross_buy_usd', Integer),
    Column('signal_ema_cross_sell_usd', Integer),
    Column('signal_rsi_buy_usd', Integer),
    Column('signal_rsi_sell_usd', Integer),
    Column('signal_macd_buy_usd', Integer),
    Column('signal_macd_sell_usd', Integer),
    Column('signal_ema50_100_buy_usd', Integer),
    Column('signal_ema50_100_sell_usd', Integer),
    Column('renko_22_usd', Text),
    Column('inscode', Text),
    Column('buy_i_volume', Double(53)),
    Column('buy_n_volume', Double(53)),
    Column('buy_i_value', Double(53)),
    Column('buy_n_value', Double(53)),
    Column('buy_n_count', Integer),
    Column('sell_i_volume', Double(53)),
    Column('buy_i_count', Integer),
    Column('sell_n_volume', Double(53)),
    Column('sell_i_value', Double(53)),
    Column('sell_n_value', Double(53)),
    Column('sell_n_count', Integer),
    Column('sell_i_count', Integer),
    Column('buy_i_value_usd', Double(53)),
    Column('buy_n_value_usd', Double(53)),
    Column('sell_i_value_usd', Double(53)),
    Column('sell_n_value_usd', Double(53)),
    Column('name_en', Text),
    Column('sector', Text),
    Column('sector_code', BigInteger),
    Column('subsector', Text),
    Column('market2', Text),
    Column('panel', Text),
    Column('share_number', BigInteger),
    Column('base_vol', BigInteger),
    Column('instrumentID', Text),
    Column('marketcap', BigInteger),
    Column('marketcap_usd', Double(53))
)


class DailyMarketSummary(Base):
    __tablename__ = 'daily_market_summary'
    __table_args__ = (
        PrimaryKeyConstraint('date', 'stock_ticker', name='daily_market_summary_pkey'),
    )

    date: Mapped[datetime.date] = mapped_column(Date, primary_key=True)
    stock_ticker: Mapped[str] = mapped_column(Text, primary_key=True)
    sector: Mapped[Optional[str]] = mapped_column(Text)
    value: Mapped[Optional[int]] = mapped_column(BigInteger)
    market_cap: Mapped[Optional[int]] = mapped_column(BigInteger)
    value_usd: Mapped[Optional[float]] = mapped_column(Double(53))
    market_cap_usd: Mapped[Optional[float]] = mapped_column(Double(53))


t_daily_market_value_summary = Table(
    'daily_market_value_summary', Base.metadata,
    Column('date', Date),
    Column('total_market_cap', Numeric),
    Column('total_market_cap_usd', Double(53)),
    Column('total_trade_value', Numeric),
    Column('total_trade_value_usd', Double(53))
)


class DailyStockData(Base):
    __tablename__ = 'daily_stock_data'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='daily_stock_data_pkey'),
        UniqueConstraint('stock_ticker', 'date_miladi', name='daily_stock_data_stock_ticker_date_miladi_key')
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    stock_ticker: Mapped[Optional[str]] = mapped_column(Text)
    j_date: Mapped[Optional[str]] = mapped_column(CHAR(10))
    date_miladi: Mapped[Optional[datetime.date]] = mapped_column(Date)
    weekday: Mapped[Optional[str]] = mapped_column(Text)
    open: Mapped[Optional[int]] = mapped_column(Integer)
    high: Mapped[Optional[int]] = mapped_column(Integer)
    low: Mapped[Optional[int]] = mapped_column(Integer)
    close: Mapped[Optional[int]] = mapped_column(Integer)
    final_price: Mapped[Optional[int]] = mapped_column(Integer)
    volume: Mapped[Optional[int]] = mapped_column(BigInteger)
    value: Mapped[Optional[int]] = mapped_column(BigInteger)
    trade_count: Mapped[Optional[int]] = mapped_column(Integer)
    name: Mapped[Optional[str]] = mapped_column(Text)
    market: Mapped[Optional[str]] = mapped_column(Text)
    adjust_open: Mapped[Optional[int]] = mapped_column(Integer)
    adjust_high: Mapped[Optional[int]] = mapped_column(Integer)
    adjust_low: Mapped[Optional[int]] = mapped_column(Integer)
    adjust_close: Mapped[Optional[int]] = mapped_column(Integer)
    adjust_final_price: Mapped[Optional[int]] = mapped_column(Integer)
    adjust_open_usd: Mapped[Optional[float]] = mapped_column(Double(53))
    adjust_high_usd: Mapped[Optional[float]] = mapped_column(Double(53))
    adjust_low_usd: Mapped[Optional[float]] = mapped_column(Double(53))
    adjust_close_usd: Mapped[Optional[float]] = mapped_column(Double(53))
    value_usd: Mapped[Optional[float]] = mapped_column(Double(53))
    dollar_rate: Mapped[Optional[float]] = mapped_column(Double(53))
    is_temp: Mapped[Optional[bool]] = mapped_column(Boolean, server_default=text('false'))


class DollarData(Base):
    __tablename__ = 'dollar_data'
    __table_args__ = (
        PrimaryKeyConstraint('date_miladi', name='dollar_data_pkey'),
    )

    date_miladi: Mapped[datetime.date] = mapped_column(Date, primary_key=True)
    open: Mapped[Optional[float]] = mapped_column(Double(53))
    high: Mapped[Optional[float]] = mapped_column(Double(53))
    low: Mapped[Optional[float]] = mapped_column(Double(53))
    close: Mapped[Optional[float]] = mapped_column(Double(53))


t_haghighi = Table(
    'haghighi', Base.metadata,
    Column('recdate', Date),
    Column('inscode', Text),
    Column('buy_i_volume', Double(53)),
    Column('buy_n_volume', Double(53)),
    Column('buy_i_value', Double(53)),
    Column('buy_n_value', Double(53)),
    Column('buy_n_count', Integer),
    Column('sell_i_volume', Double(53)),
    Column('buy_i_count', Integer),
    Column('sell_n_volume', Double(53)),
    Column('sell_i_value', Double(53)),
    Column('sell_n_value', Double(53)),
    Column('sell_n_count', Integer),
    Column('sell_i_count', Integer),
    Column('symbol', Text),
    Column('dollar_rate', Numeric),
    Column('buy_i_value_usd', Double(53)),
    Column('buy_n_value_usd', Double(53)),
    Column('sell_i_value_usd', Double(53)),
    Column('sell_n_value_usd', Double(53)),
    Column('sector', Text),
    Column('is_temp', Boolean, server_default=text('false')),
    UniqueConstraint('symbol', 'recdate', name='haghighi_symbol_recdate_unique')
)


class LiveMarketData(Base):
    __tablename__ = 'live_market_data'
    __table_args__ = (
        PrimaryKeyConstraint('Ticker', 'Download', name='live_market_data_pkey'),
    )

    Ticker: Mapped[str] = mapped_column(Text, primary_key=True)
    Download: Mapped[datetime.datetime] = mapped_column(DateTime, primary_key=True)
    Trade_Type: Mapped[Optional[str]] = mapped_column('Trade Type', Text)
    Time: Mapped[Optional[str]] = mapped_column(Text)
    Open: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    High: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    Low: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    Close: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    Final: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    Close___: Mapped[Optional[decimal.Decimal]] = mapped_column('Close(%)', Numeric)
    Final___: Mapped[Optional[decimal.Decimal]] = mapped_column('Final(%)', Numeric)
    Day_UL: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    Day_LL: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    Value: Mapped[Optional[int]] = mapped_column(BigInteger)
    BQ_Value: Mapped[Optional[int]] = mapped_column('BQ-Value', BigInteger)
    SQ_Value: Mapped[Optional[int]] = mapped_column('SQ-Value', BigInteger)
    BQPC: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    SQPC: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    Volume: Mapped[Optional[int]] = mapped_column(BigInteger)
    Vol_Buy_R: Mapped[Optional[int]] = mapped_column(BigInteger)
    Vol_Buy_I: Mapped[Optional[int]] = mapped_column(BigInteger)
    Vol_Sell_R: Mapped[Optional[int]] = mapped_column(BigInteger)
    Vol_Sell_I: Mapped[Optional[int]] = mapped_column(BigInteger)
    No: Mapped[Optional[int]] = mapped_column(Integer)
    No_Buy_R: Mapped[Optional[int]] = mapped_column(Integer)
    No_Buy_I: Mapped[Optional[int]] = mapped_column(Integer)
    No_Sell_R: Mapped[Optional[int]] = mapped_column(Integer)
    No_Sell_I: Mapped[Optional[int]] = mapped_column(Integer)
    Name: Mapped[Optional[str]] = mapped_column(Text)
    Market: Mapped[Optional[str]] = mapped_column(Text)
    Sector: Mapped[Optional[str]] = mapped_column(Text)
    Share_No: Mapped[Optional[int]] = mapped_column('Share-No', BigInteger)
    Base_Vol: Mapped[Optional[int]] = mapped_column('Base-Vol', BigInteger)
    Market_Cap: Mapped[Optional[int]] = mapped_column('Market Cap', BigInteger)
    EPS: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    updated_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime)


t_orderbook_snapshot = Table(
    'orderbook_snapshot', Base.metadata,
    Column('insCode', BigInteger),
    Column('Symbol', Text),
    Column('Timestamp', DateTime),
    Column('BuyPrice1', Numeric),
    Column('BuyVolume1', BigInteger),
    Column('SellPrice1', Numeric),
    Column('SellVolume1', BigInteger),
    Column('BuyPrice2', Numeric),
    Column('BuyVolume2', BigInteger),
    Column('SellPrice2', Numeric),
    Column('SellVolume2', BigInteger),
    Column('BuyPrice3', Numeric),
    Column('BuyVolume3', BigInteger),
    Column('SellPrice3', Numeric),
    Column('SellVolume3', BigInteger),
    Column('BuyPrice4', Numeric),
    Column('BuyVolume4', BigInteger),
    Column('SellPrice4', Numeric),
    Column('SellVolume4', BigInteger),
    Column('BuyPrice5', Numeric),
    Column('BuyVolume5', BigInteger),
    Column('SellPrice5', Numeric),
    Column('SellVolume5', BigInteger),
    Column('Sector', Text)
)


t_quote = Table(
    'quote', Base.metadata,
    Column('Day_UL', BigInteger),
    Column('Day_LL', BigInteger),
    Column('Value', BigInteger),
    Column('Time', Text),
    Column('BQ_Value', BigInteger),
    Column('SQ_Value', BigInteger),
    Column('BQPC', BigInteger),
    Column('SQPC', BigInteger),
    Column('stock_ticker', Text),
    Column('date', Text)
)


t_symboldetail = Table(
    'symboldetail', Base.metadata,
    Column('insCode', BigInteger),
    Column('name', Text),
    Column('name_en', Text),
    Column('sector', Text),
    Column('sector_code', BigInteger),
    Column('subsector', Text),
    Column('market', Text),
    Column('panel', Text),
    Column('stock_ticker', Text),
    Column('share_number', BigInteger),
    Column('base_vol', BigInteger),
    Column('instrumentID', Text)
)



class WeeklyHaghighi(Base):
    __tablename__ = 'weekly_haghighi'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='weekly_haghighi_pkey'),
        UniqueConstraint('symbol', 'week_end', name='weekly_haghighi_symbol_week_end_key')
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    symbol: Mapped[Optional[str]] = mapped_column(Text)
    week_start: Mapped[Optional[datetime.date]] = mapped_column(Date)
    week_end: Mapped[Optional[datetime.date]] = mapped_column(Date)
    buy_i_volume: Mapped[Optional[int]] = mapped_column(BigInteger)
    buy_n_volume: Mapped[Optional[int]] = mapped_column(BigInteger)
    buy_i_value: Mapped[Optional[int]] = mapped_column(BigInteger)
    buy_n_value: Mapped[Optional[int]] = mapped_column(BigInteger)
    buy_n_count: Mapped[Optional[int]] = mapped_column(BigInteger)
    sell_i_volume: Mapped[Optional[int]] = mapped_column(BigInteger)
    buy_i_count: Mapped[Optional[int]] = mapped_column(BigInteger)
    sell_n_volume: Mapped[Optional[int]] = mapped_column(BigInteger)
    sell_i_value: Mapped[Optional[int]] = mapped_column(BigInteger)
    sell_n_value: Mapped[Optional[int]] = mapped_column(BigInteger)
    sell_n_count: Mapped[Optional[int]] = mapped_column(BigInteger)
    sell_i_count: Mapped[Optional[int]] = mapped_column(BigInteger)
    buy_i_value_usd: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    buy_n_value_usd: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    sell_i_value_usd: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    sell_n_value_usd: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)


class WeeklyIndicators(Base):
    __tablename__ = 'weekly_indicators'
    __table_args__ = (
        PrimaryKeyConstraint('stock_ticker', 'week_end', name='weekly_indicators_pkey'),
    )

    stock_ticker: Mapped[str] = mapped_column(Text, primary_key=True)
    week_end: Mapped[datetime.date] = mapped_column(Date, primary_key=True)
    ema_20: Mapped[Optional[float]] = mapped_column(Double(53))
    ema_50: Mapped[Optional[float]] = mapped_column(Double(53))
    ema_100: Mapped[Optional[float]] = mapped_column(Double(53))
    rsi: Mapped[Optional[float]] = mapped_column(Double(53))
    macd: Mapped[Optional[float]] = mapped_column(Double(53))
    macd_signal: Mapped[Optional[float]] = mapped_column(Double(53))
    macd_hist: Mapped[Optional[float]] = mapped_column(Double(53))
    tenkan: Mapped[Optional[float]] = mapped_column(Double(53))
    kijun: Mapped[Optional[float]] = mapped_column(Double(53))
    senkou_a: Mapped[Optional[float]] = mapped_column(Double(53))
    senkou_b: Mapped[Optional[float]] = mapped_column(Double(53))
    chikou: Mapped[Optional[float]] = mapped_column(Double(53))
    signal_ichimoku_buy: Mapped[Optional[int]] = mapped_column(SmallInteger)
    signal_ichimoku_sell: Mapped[Optional[int]] = mapped_column(SmallInteger)
    signal_ema_cross_buy: Mapped[Optional[int]] = mapped_column(SmallInteger)
    signal_ema_cross_sell: Mapped[Optional[int]] = mapped_column(SmallInteger)
    signal_rsi_buy: Mapped[Optional[int]] = mapped_column(SmallInteger)
    signal_rsi_sell: Mapped[Optional[int]] = mapped_column(SmallInteger)
    signal_macd_buy: Mapped[Optional[int]] = mapped_column(SmallInteger)
    signal_macd_sell: Mapped[Optional[int]] = mapped_column(SmallInteger)
    signal_ema50_100_buy: Mapped[Optional[int]] = mapped_column(SmallInteger)
    signal_ema50_100_sell: Mapped[Optional[int]] = mapped_column(SmallInteger)
    atr_52: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    renko_52: Mapped[Optional[str]] = mapped_column(Text)
    ema_20_d: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    ema_50_d: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    ema_100_d: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    rsi_d: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    macd_d: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    macd_signal_d: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    macd_hist_d: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    tenkan_d: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    kijun_d: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    senkou_a_d: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    senkou_b_d: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    chikou_d: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    signal_ichimoku_buy_d: Mapped[Optional[int]] = mapped_column(Integer)
    signal_ichimoku_sell_d: Mapped[Optional[int]] = mapped_column(Integer)
    signal_ema_cross_buy_d: Mapped[Optional[int]] = mapped_column(Integer)
    signal_ema_cross_sell_d: Mapped[Optional[int]] = mapped_column(Integer)
    signal_rsi_buy_d: Mapped[Optional[int]] = mapped_column(Integer)
    signal_rsi_sell_d: Mapped[Optional[int]] = mapped_column(Integer)
    signal_macd_buy_d: Mapped[Optional[int]] = mapped_column(Integer)
    signal_macd_sell_d: Mapped[Optional[int]] = mapped_column(Integer)
    signal_ema50_100_buy_d: Mapped[Optional[int]] = mapped_column(Integer)
    signal_ema50_100_sell_d: Mapped[Optional[int]] = mapped_column(Integer)
    atr_52_d: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    renko_52_d: Mapped[Optional[str]] = mapped_column(Text)


t_weekly_joined_data = Table(
    'weekly_joined_data', Base.metadata,
    Column('id', Integer),
    Column('stock_ticker', Text),
    Column('week_start', Date),
    Column('week_end', Date),
    Column('open', Integer),
    Column('high', Integer),
    Column('low', Integer),
    Column('close', Integer),
    Column('final_price', Integer),
    Column('adjust_open', Integer),
    Column('adjust_high', Integer),
    Column('adjust_low', Integer),
    Column('adjust_close', Integer),
    Column('adjust_final_price', Integer),
    Column('volume', BigInteger),
    Column('value', BigInteger),
    Column('name', Text),
    Column('market', Text),
    Column('adjust_open_usd', Numeric),
    Column('adjust_high_usd', Numeric),
    Column('adjust_low_usd', Numeric),
    Column('adjust_close_usd', Numeric),
    Column('value_usd', Numeric),
    Column('dollar_rate', Numeric),
    Column('ema_20', Double(53)),
    Column('ema_50', Double(53)),
    Column('ema_100', Double(53)),
    Column('rsi', Double(53)),
    Column('macd', Double(53)),
    Column('macd_signal', Double(53)),
    Column('macd_hist', Double(53)),
    Column('tenkan', Double(53)),
    Column('kijun', Double(53)),
    Column('senkou_a', Double(53)),
    Column('senkou_b', Double(53)),
    Column('chikou', Double(53)),
    Column('signal_ichimoku_buy', SmallInteger),
    Column('signal_ichimoku_sell', SmallInteger),
    Column('signal_ema_cross_buy', SmallInteger),
    Column('signal_ema_cross_sell', SmallInteger),
    Column('signal_rsi_buy', SmallInteger),
    Column('signal_rsi_sell', SmallInteger),
    Column('signal_macd_buy', SmallInteger),
    Column('signal_macd_sell', SmallInteger),
    Column('signal_ema50_100_buy', SmallInteger),
    Column('signal_ema50_100_sell', SmallInteger),
    Column('atr_52', Numeric),
    Column('renko_52', Text),
    Column('ema_20_d', Numeric),
    Column('ema_50_d', Numeric),
    Column('ema_100_d', Numeric),
    Column('rsi_d', Numeric),
    Column('macd_d', Numeric),
    Column('macd_signal_d', Numeric),
    Column('macd_hist_d', Numeric),
    Column('tenkan_d', Numeric),
    Column('kijun_d', Numeric),
    Column('senkou_a_d', Numeric),
    Column('senkou_b_d', Numeric),
    Column('chikou_d', Numeric),
    Column('signal_ichimoku_buy_d', Integer),
    Column('signal_ichimoku_sell_d', Integer),
    Column('signal_ema_cross_buy_d', Integer),
    Column('signal_ema_cross_sell_d', Integer),
    Column('signal_rsi_buy_d', Integer),
    Column('signal_rsi_sell_d', Integer),
    Column('signal_macd_buy_d', Integer),
    Column('signal_macd_sell_d', Integer),
    Column('signal_ema50_100_buy_d', Integer),
    Column('signal_ema50_100_sell_d', Integer),
    Column('atr_52_d', Numeric),
    Column('renko_52_d', Text),
    Column('buy_i_volume', BigInteger),
    Column('buy_n_volume', BigInteger),
    Column('buy_i_value', BigInteger),
    Column('buy_n_value', BigInteger),
    Column('buy_n_count', BigInteger),
    Column('sell_i_volume', BigInteger),
    Column('buy_i_count', BigInteger),
    Column('sell_n_volume', BigInteger),
    Column('sell_i_value', BigInteger),
    Column('sell_n_value', BigInteger),
    Column('sell_n_count', BigInteger),
    Column('sell_i_count', BigInteger),
    Column('name_en', Text),
    Column('sector', Text),
    Column('sector_code', BigInteger),
    Column('subsector', Text),
    Column('panel', Text),
    Column('share_number', BigInteger),
    Column('base_vol', BigInteger),
    Column('instrumentID', Text),
    Column('marketcap', BigInteger),
    Column('marketcap_usd', Numeric)
)


class WeeklyStockData(Base):
    __tablename__ = 'weekly_stock_data'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='weekly_stock_data_pkey'),
        UniqueConstraint('stock_ticker', 'week_end', name='weekly_stock_data_stock_ticker_week_end_key')
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    stock_ticker: Mapped[Optional[str]] = mapped_column(Text)
    week_start: Mapped[Optional[datetime.date]] = mapped_column(Date)
    week_end: Mapped[Optional[datetime.date]] = mapped_column(Date)
    open: Mapped[Optional[int]] = mapped_column(Integer)
    high: Mapped[Optional[int]] = mapped_column(Integer)
    low: Mapped[Optional[int]] = mapped_column(Integer)
    close: Mapped[Optional[int]] = mapped_column(Integer)
    final_price: Mapped[Optional[int]] = mapped_column(Integer)
    adjust_open: Mapped[Optional[int]] = mapped_column(Integer)
    adjust_high: Mapped[Optional[int]] = mapped_column(Integer)
    adjust_low: Mapped[Optional[int]] = mapped_column(Integer)
    adjust_close: Mapped[Optional[int]] = mapped_column(Integer)
    adjust_final_price: Mapped[Optional[int]] = mapped_column(Integer)
    volume: Mapped[Optional[int]] = mapped_column(BigInteger)
    value: Mapped[Optional[int]] = mapped_column(BigInteger)
    name: Mapped[Optional[str]] = mapped_column(Text)
    market: Mapped[Optional[str]] = mapped_column(Text)
    adjust_open_usd: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    adjust_high_usd: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    adjust_low_usd: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    adjust_close_usd: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    value_usd: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    dollar_rate: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)


