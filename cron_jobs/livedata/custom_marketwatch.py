import re
import requests
import pandas as pd
import jdatetime


HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
    "Connection": "keep-alive",
}


def ar_to_fa(text):
    if text is None:
        return None
    text = str(text)
    return text.replace("ي", "ی").replace("ك", "ک")


def normalize_text(text):
    if text is None:
        return None
    text = ar_to_fa(text)
    text = text.replace("\u200c", " ")
    return text.strip()


def safe_to_numeric(df, cols):
    for col in cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def Get_MarketWatch(save_excel=True, save_path='D:/FinPy-TSE Data/MarketWatch'):
    # ------------------------------------------------------------------
    # GET MARKET RETAIL AND INSTITUTIONAL DATA
    # ------------------------------------------------------------------
    r = requests.get('http://old.tsetmc.com/tsev2/data/ClientTypeAll.aspx', headers=HEADERS, timeout=30)
    r.raise_for_status()

    mkt_ri_df = pd.DataFrame(r.text.split(';'))
    mkt_ri_df = mkt_ri_df[0].str.split(",", expand=True)

    mkt_ri_df.columns = [
        'WEB-ID', 'No_Buy_R', 'No_Buy_I', 'Vol_Buy_R', 'Vol_Buy_I',
        'No_Sell_R', 'No_Sell_I', 'Vol_Sell_R', 'Vol_Sell_I'
    ]

    ri_cols = [
        'No_Buy_R', 'No_Buy_I', 'Vol_Buy_R', 'Vol_Buy_I',
        'No_Sell_R', 'No_Sell_I', 'Vol_Sell_R', 'Vol_Sell_I'
    ]
    mkt_ri_df = safe_to_numeric(mkt_ri_df, ri_cols)
    mkt_ri_df['WEB-ID'] = mkt_ri_df['WEB-ID'].astype(str).str.strip()
    mkt_ri_df = mkt_ri_df.set_index('WEB-ID')
    mkt_ri_df = mkt_ri_df[
        ['No_Buy_R', 'No_Buy_I', 'No_Sell_R', 'No_Sell_I',
         'Vol_Buy_R', 'Vol_Buy_I', 'Vol_Sell_R', 'Vol_Sell_I']
    ]

    # ------------------------------------------------------------------
    # GET MARKET WATCH PRICE AND ORDERBOOK RAW DATA
    # ------------------------------------------------------------------
    r = requests.get('http://old.tsetmc.com/tsev2/data/MarketWatchPlus.aspx', headers=HEADERS, timeout=30)
    r.raise_for_status()
    main_text = r.text

    # -------------------- Market watch table ---------------------------
    mkt_df = pd.DataFrame((main_text.split('@')[2]).split(';'))
    mkt_df = mkt_df[0].str.split(",", expand=True)
    mkt_df = mkt_df.iloc[:, :23]

    mkt_df.columns = [
        'WEB-ID', 'Ticker-Code', 'Ticker', 'Name', 'Time', 'Open', 'Final', 'Close',
        'No', 'Volume', 'Value', 'Low', 'High', 'Y-Final', 'EPS', 'Base-Vol',
        'Unknown1', 'Unknown2', 'Sector', 'Day_UL', 'Day_LL', 'Share-No', 'Mkt-ID'
    ]

    mkt_df = mkt_df[
        ['WEB-ID', 'Ticker', 'Name', 'Time', 'Open', 'Final', 'Close', 'No', 'Volume',
         'Value', 'Low', 'High', 'Y-Final', 'EPS', 'Base-Vol', 'Sector', 'Day_UL',
         'Day_LL', 'Share-No', 'Mkt-ID']
    ]

    # ------------------------------------------------------------------
    # IMPORTANT:
    # keep derivatives/options too
    # ------------------------------------------------------------------
    mkt_id_list = [
        '300', '303', '305', '309', '400', '403', '404',
        '306', '311', '312', '320', '321', '380'
    ]
    mkt_df = mkt_df[mkt_df['Mkt-ID'].isin(mkt_id_list)].copy()

    market_map = {
        '300': 'بورس',
        '303': 'فرابورس',
        '305': 'صندوق قابل معامله',
        '306': 'بازار مشتقه',
        '309': 'پایه',
        '311': 'اختیار معامله',
        '312': 'اختیار معامله',
        '320': 'اختیار معامله',
        '321': 'اختیار معامله',
        '380': 'اختیار معامله',
        '400': 'حق تقدم بورس',
        '403': 'حق تقدم فرابورس',
        '404': 'حق تقدم پایه',
    }
    mkt_df['Market'] = mkt_df['Mkt-ID'].map(market_map)

    # assign sector names
    r = requests.get('https://cdn.tsetmc.com/api/StaticData/GetStaticData', headers=HEADERS, timeout=30)
    r.raise_for_status()
    sec_df = pd.DataFrame(r.json()['staticData'])
    sec_df['code'] = sec_df['code'].astype(str).apply(lambda x: '0' + x if len(x) == 1 else x)
    sec_df['name'] = sec_df['name'].apply(normalize_text)
    sec_df = sec_df[sec_df['type'] == 'IndustrialGroup'][['code', 'name']]

    mkt_df['Sector'] = mkt_df['Sector'].map(dict(sec_df[['code', 'name']].values))

    # numeric conversions
    num_cols = [
        'Open', 'Final', 'Close', 'No', 'Volume', 'Value', 'Low', 'High',
        'Y-Final', 'EPS', 'Base-Vol', 'Day_UL', 'Day_LL', 'Share-No'
    ]
    mkt_df = safe_to_numeric(mkt_df, num_cols)

    # text cleanup
    mkt_df['Time'] = mkt_df['Time'].astype(str).apply(
        lambda x: x[:-4] + ':' + x[-4:-2] + ':' + x[-2:] if len(x) >= 6 else x
    )
    mkt_df['Ticker'] = mkt_df['Ticker'].apply(normalize_text)
    mkt_df['Name'] = mkt_df['Name'].apply(normalize_text)



    # derived metrics
    mkt_df['Close(%)'] = round((mkt_df['Close'] - mkt_df['Y-Final']) / mkt_df['Y-Final'] * 100, 2)
    mkt_df['Final(%)'] = round((mkt_df['Final'] - mkt_df['Y-Final']) / mkt_df['Y-Final'] * 100, 2)
    mkt_df['Market Cap'] = round(mkt_df['Share-No'] * mkt_df['Final'], 2)

    # clean web-id
    mkt_df['WEB-ID'] = mkt_df['WEB-ID'].astype(str).str.strip()
    mkt_df = mkt_df.set_index('WEB-ID')

    # ------------------------------------------------------------------
    # ORDERBOOK RAW
    # ------------------------------------------------------------------
    ob_df = pd.DataFrame((main_text.split('@')[3]).split(';'))
    ob_df = ob_df[0].str.split(",", expand=True)

    ob_df.columns = ['WEB-ID', 'OB-Depth', 'Sell-No', 'Buy-No', 'Buy-Price', 'Sell-Price', 'Buy-Vol', 'Sell-Vol']
    ob_df = ob_df[['WEB-ID', 'OB-Depth', 'Sell-No', 'Sell-Vol', 'Sell-Price', 'Buy-Price', 'Buy-Vol', 'Buy-No']]

    # top row orderbook
    ob1_df = ob_df[ob_df['OB-Depth'] == '1'].copy()
    ob1_df.drop(columns=['OB-Depth'], inplace=True)
    ob1_df['WEB-ID'] = ob1_df['WEB-ID'].astype(str).str.strip()
    ob1_df = ob1_df.set_index('WEB-ID')

    ob_num_cols = ['Sell-No', 'Sell-Vol', 'Sell-Price', 'Buy-Price', 'Buy-Vol', 'Buy-No']
    ob1_df = safe_to_numeric(ob1_df, ob_num_cols)

    # join top-of-book to market table
    mkt_df = mkt_df.join(ob1_df)

    # queue values
    bq_value = mkt_df.apply(
        lambda x: int(x['Buy-Vol'] * x['Buy-Price'])
        if pd.notna(x['Buy-Vol']) and pd.notna(x['Buy-Price']) and pd.notna(x['Day_UL']) and x['Buy-Price'] == x['Day_UL']
        else 0,
        axis=1
    )
    sq_value = mkt_df.apply(
        lambda x: int(x['Sell-Vol'] * x['Sell-Price'])
        if pd.notna(x['Sell-Vol']) and pd.notna(x['Sell-Price']) and pd.notna(x['Day_LL']) and x['Sell-Price'] == x['Day_LL']
        else 0,
        axis=1
    )
    mkt_df = pd.concat(
        [mkt_df, pd.DataFrame(bq_value, columns=['BQ-Value']), pd.DataFrame(sq_value, columns=['SQ-Value'])],
        axis=1
    )

    # per-capita queue
    bq_pc_avg = mkt_df.apply(
        lambda x: int(round(x['BQ-Value'] / x['Buy-No'], 0))
        if pd.notna(x['BQ-Value']) and pd.notna(x['Buy-No']) and x['BQ-Value'] != 0 and x['Buy-No'] != 0
        else 0,
        axis=1
    )
    sq_pc_avg = mkt_df.apply(
        lambda x: int(round(x['SQ-Value'] / x['Sell-No'], 0))
        if pd.notna(x['SQ-Value']) and pd.notna(x['Sell-No']) and x['SQ-Value'] != 0 and x['Sell-No'] != 0
        else 0,
        axis=1
    )
    mkt_df = pd.concat(
        [mkt_df, pd.DataFrame(bq_pc_avg, columns=['BQPC']), pd.DataFrame(sq_pc_avg, columns=['SQPC'])],
        axis=1
    )

    # ------------------------------------------------------------------
    # JOIN MARKET + CLIENT TYPE
    # ------------------------------------------------------------------
    final_df = mkt_df.join(mkt_ri_df)

    # trade type
    final_df['Trade Type'] = final_df['Ticker'].apply(
        lambda x: 'تابلو' if ((not str(x)[-1].isdigit()) or (x in ['انرژی1', 'انرژی2', 'انرژی3']))
        else ('بلوکی' if str(x)[-1] == '2'
              else ('عمده' if str(x)[-1] == '4'
                    else ('جبرانی' if str(x)[-1] == '3' else 'تابلو')))
    )

    # download timestamp (jalali)
    jdatetime_download = jdatetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
    final_df['Download'] = jdatetime_download

    final_df = final_df[
        [
            'Ticker', 'Trade Type', 'Time', 'Open', 'High', 'Low', 'Close', 'Final',
            'Close(%)', 'Final(%)', 'Day_UL', 'Day_LL', 'Value', 'BQ-Value', 'SQ-Value',
            'BQPC', 'SQPC', 'Volume', 'Vol_Buy_R', 'Vol_Buy_I', 'Vol_Sell_R', 'Vol_Sell_I',
            'No', 'No_Buy_R', 'No_Buy_I', 'No_Sell_R', 'No_Sell_I', 'Name', 'Market',
            'Sector', 'Share-No', 'Base-Vol', 'Market Cap', 'EPS','Download'
        ]
    ]

    final_df = final_df.set_index('Ticker')

    # ------------------------------------------------------------------
    # FULL ORDERBOOK OUTPUT
    # ------------------------------------------------------------------
    final_ob_df = (mkt_df[['Ticker', 'Day_LL', 'Day_UL']]).join(ob_df.set_index('WEB-ID'))

    final_ob_num_cols = [
        'Day_LL', 'Day_UL', 'OB-Depth', 'Sell-No', 'Sell-Vol',
        'Sell-Price', 'Buy-Price', 'Buy-Vol', 'Buy-No'
    ]
    final_ob_df = safe_to_numeric(final_ob_df, final_ob_num_cols)

    final_ob_df = final_ob_df.sort_values(['Ticker', 'OB-Depth'], ascending=(True, True))
    final_ob_df = final_ob_df.set_index(['Ticker', 'Day_LL', 'Day_UL', 'OB-Depth'])
    final_ob_df['Download'] = jdatetime_download

    # ------------------------------------------------------------------
    # SAVE IF REQUESTED
    # ------------------------------------------------------------------
    if save_excel:
        try:
            if save_path[-1] != '/':
                save_path = save_path + '/'

            mkt_watch_file_name = 'MarketWatch ' + jdatetime.datetime.today().strftime("%Y-%m-%d %H-%M-%S")
            ob_file_name = 'OrderBook ' + jdatetime.datetime.today().strftime("%Y-%m-%d %H-%M-%S")

            final_ob_df.to_excel(save_path + ob_file_name + '.xlsx')
            final_df.to_excel(save_path + mkt_watch_file_name + '.xlsx')
        except Exception:
            print('Save path does not exist. You can save returned dataframes manually using .to_excel().')

    return final_df, final_ob_df