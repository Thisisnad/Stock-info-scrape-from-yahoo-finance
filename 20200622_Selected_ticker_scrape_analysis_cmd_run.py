from datetime import datetime
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import ssl
import os

pd.set_option('mode.chained_assignment', None)

""" Work directory set """

path = os.getcwd()
print('Initial working directory: ' + path.strip())

os.chdir('C:\\Anaconda\\PyCharmProjects\\202006_Selected_Page_Scraping_Project')
new_path = os.getcwd()
print('New/ Current working directory: ' + new_path.strip())

print('\n')

""" START CODE """

''' Input Parameters '''

ticker_list = ['AAPL', 'AMD', 'AMZN', 'BA', 'BABA', 'CAT', 'CHA', 'CHL', 'COP', 'CRM', 'CSCO', 'DIS'
                , 'EBAY', 'EXPN.L', 'EZJ.L', 'FB', 'GOOG', 'GOOGL', 'GS', 'GSK.L', 'HPQ', 'HSBA.L'
                , 'IAG.L', 'IBM', 'INTC', 'JPM', 'KLAC', 'LMT', 'MONY.L', 'MSFT', 'MU', 'NFLX'
                , 'NKE', 'NVDA', 'OCDO.L', 'ORCL', 'PAY.L', 'PYPL', 'QCOM', 'RKET.DE', 'RMV.L'
                , 'ROKU', 'RYA.L', 'SAP', 'SBRY.L', 'SONO', 'SPCE', 'SPOT', 'SVMK', 'T', 'TSCO.L'
                , 'TSLA', 'TWTR', 'ULVR.L', 'VRSN', 'VZ', 'WIZZ.L', 'WMT']

base_url = 'https://finance.yahoo.com/quote/'

input_scrape_csv = 'Selected_ticker_list_scrape.csv'
output_data_csv = 'Selected_ticker_data_prep.csv'


''' Phase - 1: Scrape selected ticker info into PD dataframe/ CSV file '''

''' Time of Run '''
runtime = datetime.now().strftime('%Y/%m/%d [%H:%M]')

for i, ticker in enumerate(ticker_list):

    ticker = ticker_list[i]
    url = base_url + ticker

    # Ignore SSL certificate errors
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    # html = urllib.request.urlopen(url, context=ctx).read()
    html = requests.get(url, "html.parser").content
    soup = BeautifulSoup(html, 'html.parser')
    pretty_soup = soup.prettify()
    soup_text = soup.text

    ''' Set Index - Date/ Ticker '''
    index = [runtime, ticker]
    # print(index)

    ''' Company Name '''
    comp_name_1 = soup.h1

    if comp_name_1 == None:
        print('Company Name [None object error!]: ' + url)
    else:
        comp_name = comp_name_1.text
    # print(comp_name)

    ''' Market/ Currency '''
    # market_1 = soup.find('div', class_='C($tertiaryColor) Fz(12px)').text
    market_1 = soup.find('div', class_='C($tertiaryColor) Fz(12px)')

    if market_1 == None:
        print('Market/ Currency [None object error!]: ' + url)
    else:
        market = market_1.text.split(' ')[0]
        currency = market_1.text.split(' ')[-1]
    # print(market)
    # print(currency)

    ''' Current Price/ Price Change '''
    curr_price_info = soup.find_all('span', class_='Trsdu(0.3s)')

    if curr_price_info == None:
        print('Current Price/ Price Change [None object error!]: ' + url)
    else:
        curr_pr = curr_price_info[0].text
        # curr_pr = float(curr_pr)
        curr_pr_ch = curr_price_info[1].text

    ''' Market Time '''
    m_time1 = soup.find('div', id='quote-market-notice')

    if m_time1 == None:
        print('Market Time [None object error!]: ' + url)
    else:
        m_time = m_time1.text

    ''' Other Daily Metrics '''
    metrics_h = soup.find_all('td', class_='C($primaryColor) W(51%)')

    if metrics_h == None:
        print('Other Daily Metrics [None object error!]: ' + url)
    else:
        len_metrics_h = 0
        list_metrics_h = []
        for i in metrics_h:
            len_metrics_h += 1
            text = i.text
            list_metrics_h.append(text)

    metrics = soup.find_all('td', class_='Ta(end) Fw(600) Lh(14px)')

    if metrics == None:
        print('Other Daily Metrics [None object error!]: ' + url)
    else:
        len_metrics = 0
        list_metrics = []
        for i in metrics:
            len_metrics += 1
            text = i.text
            list_metrics.append(text)

    ''' Create Pandas DataFrame '''

    header_list = ['Runtime', 'Ticker', 'Company Name', 'Market', 'Currency'
        , 'Current Price', 'Price Change', 'Market Time']
    val_list = [runtime, ticker, comp_name, market, currency, curr_pr, curr_pr_ch, m_time]

    for i, val in enumerate(list_metrics_h):
        header_list.append(val)

    for i, val in enumerate(list_metrics):
        val_list.append(val)

    stock_pr_dict = {key: value for key, value in zip(header_list, val_list)}
    stock_price_df = pd.DataFrame(stock_pr_dict, index=[runtime])
    # print(stock_price_df)

    ''' Write to CSV File'''

    def appendDFToCSV_void(df, csvFilePath, sep=","):
        if not os.path.isfile(csvFilePath):
            df.to_csv(csvFilePath, mode='a', index=False, sep=sep, header=True)
        elif len(df.columns) != len(pd.read_csv(csvFilePath, nrows=1, sep=sep).columns):
            raise Exception(
                "Columns do not match!! Dataframe has " + str(len(df.columns)) + " columns. CSV file has " + str(
                    len(pd.read_csv(csvFilePath, nrows=1, sep=sep).columns)) + " columns.")
        elif not (df.columns == pd.read_csv(csvFilePath, nrows=1, sep=sep).columns).all():
            raise Exception("Columns and column order of dataframe and csv file do not match!!")
        else:
            df.to_csv(csvFilePath, mode='a', index=False, sep=sep, header=False)


    try:
        appendDFToCSV_void(stock_price_df, input_scrape_csv, sep=",")
    except:
        print('Selected ticker list can\'t be scraped/ saved into csv!!!')


''' Phase - 2: Prepare/ clean scraped data into CSV file '''

''' Read CSV File'''

stock_price_df = pd.read_csv(input_scrape_csv, parse_dates=True)
# stock_price_df = pd.read_csv(filename, index_col=['Runtime', 'Ticker'], parse_dates=True)

''' Subset/ rename columns in a new dataframe '''
stock_price_df_1 = stock_price_df[['Runtime', 'Ticker', 'Company Name', 'Market', 'Currency',
       'Current Price', 'Price Change', 'Market Time', 'Previous Close',
       'Open', 'Day\'s Range', '52 Week Range', 'Market Cap', 'Beta (5Y Monthly)',
       'PE Ratio (TTM)', 'EPS (TTM)', 'Earnings Date', 'Forward Dividend & Yield',
       'Ex-Dividend Date', '1y Target Est']]
stock_price_df_1.columns = ['runtime', 'ticker', 'company_name', 'market', 'currency',
       'current_price', 'price_change', 'market_time', 'previous_close',
       'open', 'day_range', 'week_52_range', 'market_cap', 'beta_5y_monthly',
       'pe_ratio', 'eps', 'earnings_date', 'div_yield', 'ex_div_date', 'target_est_1_yr']

''' Format dates and set index '''

# stock_price_df_1['runtime'] = pd.to_datetime(stock_price_df_1['runtime'], format='%Y/%m/%d [%H:%M]', errors='coerce')
stock_price_df_1['runtime'] = stock_price_df_1['runtime']\
                            .apply(lambda x: datetime.strptime(x, '%Y/%m/%d [%H:%M]'))

# Inspect data
print('\n--------------- Data Head ---------------')
print(stock_price_df_1.head())
print('\n--------------- Shape ---------------')
print('Row: ' + str(stock_price_df_1.shape[0]))
print('Column: ' + str(stock_price_df_1.shape[1]))
print('\n--------------- Columns ---------------')
print(stock_price_df_1.columns)
# print('\n--------------- Index Name ---------------')
# print(stock_price_df_1.index.names)
# print('\n--------------- Info ---------------')
# print(stock_price_df_1.info())
# print('\n--------------- Data Type ---------------')
# print(stock_price_df_1.dtypes)
# print(stock_price_df_1['runtime'].dtype)
print('\n--------------- Missing ---------------')
print(stock_price_df_1.isna().sum())
# print(stock_price_df_1.isna().any())

''' Format/ split/ define new columns - data prep '''

# Current price / Previous close / open/ Target estimate

to_float_list = ['current_price', 'previous_close', 'open', 'target_est_1_yr']

for i, val in enumerate(to_float_list):
       stock_price_df_1[val] = stock_price_df_1[val].replace('[,]', '', regex=True)
       stock_price_df_1[val] = pd.to_numeric(stock_price_df_1[val])
       stock_price_df_1[val].astype('float')
       # print(stock_price_df_1[val].head())

# Price change -> absolute/ percentage change
change_split = stock_price_df_1['price_change'].str.split(' ')

stock_price_df_1['pr_change_abs'] = change_split.str.get(0)
stock_price_df_1['pr_change_abs'] = pd.to_numeric(stock_price_df_1['pr_change_abs'])
stock_price_df_1['pr_change_abs'].astype('float')

stock_price_df_1['pr_change_pct'] = change_split.str.get(-1)
stock_price_df_1['pr_change_pct'] = stock_price_df_1['pr_change_pct'].replace('[()%]', '', regex=True)
stock_price_df_1['pr_change_pct'] = pd.to_numeric(stock_price_df_1['pr_change_pct'])
stock_price_df_1['pr_change_pct'].astype('float')

# Market time
m_time_split = stock_price_df_1['market_time'].str.split(' ')
stock_price_df_1['m_time_ind'] = m_time_split.str.get(0) + m_time_split.str.get(1)
stock_price_df_1['m_price_time'] = m_time_split.str.get(3) + ' [' \
                                + m_time_split.str.get(4).replace('[.]', '', regex=True) + ']'
stock_price_df_1['market_status'] = stock_price_df_1.apply(lambda x: 'Open' if x.m_time_ind == 'Asof' else 'Close', axis=1)

# Day's range
day_range_split = stock_price_df_1['day_range'].str.split('-')
stock_price_df_1['day_range_lo'] = day_range_split.str.get(0)
stock_price_df_1['day_range_hi'] = day_range_split.str.get(-1)

to_float_list = ['day_range_lo', 'day_range_hi']

for i, val in enumerate(to_float_list):
       stock_price_df_1[val] = stock_price_df_1[val].replace('[,]', '', regex=True)
       stock_price_df_1[val] = pd.to_numeric(stock_price_df_1[val])
       stock_price_df_1[val].astype('float')
       # print(stock_price_df_1[val].head())

# 52-Weeks Range
weeks_range_split = stock_price_df_1['week_52_range'].str.split('-')
stock_price_df_1['wk52_range_lo'] = weeks_range_split.str.get(0)
stock_price_df_1['wk52_range_hi'] = weeks_range_split.str.get(-1)

to_float_list = ['wk52_range_lo', 'wk52_range_hi']

for i, val in enumerate(to_float_list):
       stock_price_df_1[val] = stock_price_df_1[val].replace('[,]', '', regex=True)
       stock_price_df_1[val] = pd.to_numeric(stock_price_df_1[val])
       stock_price_df_1[val].astype('float')
       # print(stock_price_df_1[val].head())

# Earnings Date/ Ex-dividend date
earnings_date_split = stock_price_df_1['earnings_date'].str.split('-')
stock_price_df_1['earnings_date'] = earnings_date_split.str.get(-1)

to_date_list = ['earnings_date', 'ex_div_date']

for i, val in enumerate(to_date_list):
       stock_price_df_1[val] = stock_price_df_1[val].replace('\s', '', regex=True)
       stock_price_df_1[val] = pd.to_datetime(stock_price_df_1[val], format='%b%d,%Y', errors='coerce')
       # print(stock_price_df_1[val])

# Forward dividend/ Dividend yield
stock_price_df_1['div_yield'] = stock_price_df_1['div_yield'].replace('N/A', '', regex=True)
stock_price_df_1['div_yield'] = stock_price_df_1['div_yield'].replace('[()%]', '', regex=True)
dividend_split = stock_price_df_1['div_yield'].str.split(' ')
stock_price_df_1['forward_div_ps'] = dividend_split.str.get(0)
stock_price_df_1['div_yield_pct'] = dividend_split.str.get(-1)

to_float_list = ['forward_div_ps', 'div_yield_pct']

for i, val in enumerate(to_float_list):
       stock_price_df_1[val] = pd.to_numeric(stock_price_df_1[val])
       stock_price_df_1[val].astype('float')
       # print(stock_price_df_1[val])

# Market Cap
stock_price_df_1['market_cap'] = stock_price_df_1['market_cap'].replace('M', 'xMn', regex=True)
stock_price_df_1['market_cap'] = stock_price_df_1['market_cap'].replace('B', 'xBn', regex=True)
stock_price_df_1['market_cap'] = stock_price_df_1['market_cap'].replace('T', 'xTn', regex=True)
market_cap_split = stock_price_df_1['market_cap'].str.split('x')
stock_price_df_1['market_cap_1'] = market_cap_split.str.get(0)
stock_price_df_1['market_cap_2'] = market_cap_split.str.get(-1)
stock_price_df_1['market_cap_1'] = stock_price_df_1['market_cap_1'].replace('[,]', '', regex=True)
# stock_price_df_1['market_cap_1'] = pd.to_numeric(stock_price_df_1['market_cap_1'])
# stock_price_df_1['market_cap_1'].astype('float')
stock_price_df_1['market_cap_1'] = stock_price_df_1['market_cap_1']\
                            .apply(lambda x: pd.to_numeric(x))

condition_1 = (stock_price_df_1['market_cap_2'] == 'Mn')
condition_2 = (stock_price_df_1['market_cap_2'] == 'Bn')
condition_3 = (stock_price_df_1['market_cap_2'] == 'Tn')
conditions = [condition_1, condition_2, condition_3]
choices = [stock_price_df_1['market_cap_1']*1000000, stock_price_df_1['market_cap_1']*1000000000, stock_price_df_1['market_cap_1']*1000000000000]

stock_price_df_1["market_cap"] = np.select(conditions, choices, default=stock_price_df_1['market_cap_1'])
stock_price_df_1['market_cap_m'] = stock_price_df_1['market_cap'].apply(lambda x: x/1000000)

# Format/ order columns for new dataset
stock_price_df_1.drop(['price_change', 'm_time_ind', 'day_range', 'week_52_range', 'div_yield', 'market_cap_1'
                          , 'market_time', 'market_cap_2', 'market_cap']
                   , axis=1, inplace=True)

stock_price_df_1 = stock_price_df_1[['runtime', 'ticker', 'company_name', 'market', 'currency', 'm_price_time'
                     , 'market_status', 'current_price', 'previous_close', 'open', 'pr_change_abs'
                     , 'pr_change_pct', 'day_range_lo', 'day_range_hi', 'wk52_range_lo', 'wk52_range_hi'
                     , 'target_est_1_yr', 'earnings_date', 'ex_div_date', 'forward_div_ps'
                     , 'div_yield_pct', 'market_cap_m', 'pe_ratio', 'eps', 'beta_5y_monthly']]

print('\n--------------- Shape ---------------')
print('Row: ' + str(stock_price_df_1.shape[0]))
print('Column: ' + str(stock_price_df_1.shape[1]))
print('\n--------------- Data Type ---------------')
print(stock_price_df_1.dtypes)
print('\n--------------- Columns ---------------')
print(stock_price_df_1.columns)

''' Write to CSV File'''

def appendDFToCSV_void(df, csvFilePath, sep=","):
       if not os.path.isfile(csvFilePath):
              df.to_csv(csvFilePath, mode='a', index=False, sep=sep, header=True)
       else:
              os.remove(csvFilePath)
              df.to_csv(csvFilePath, mode='a', index=False, sep=sep, header=True)

try:
       appendDFToCSV_void(stock_price_df_1, output_data_csv, sep=",")
except:
       print('\n - - - - - - Warning! - - - - - -')
       print('The CSV file can\'t be written!!!')