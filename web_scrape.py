import pandas as pd
from sqlalchemy import *
import requests
from bs4 import BeautifulSoup
import datetime
from time import *
import yfinance as yf




def PullStockMovingAvgs(ticker, api_key, time_period, resolution_CS, start_year, start_month, start_day):
    #This converts the dates into Epoch time for input into the code
    start_timeseries = round(datetime.datetime(start_year,start_month,start_day).timestamp())
    end_timeseries = round(time())
    #dates = pd.to_datetime(start_timeseries,unit='s')
    #This also pulls the stock date, open, close, high, low, and volume
    SMA = requests.get(f'https://finnhub.io/api/v1/indicator?symbol={ticker}&resolution={resolution_CS}&from={start_timeseries}&to={end_timeseries}&indicator=sma&timeperiod={time_period}&token={api_key}').json()
    timestamp = SMA['t']
    sma_calc = SMA['sma']
    open_price = SMA['o']
    close_price = SMA['c']
    high_price = SMA['h']
    low_price = SMA['l']
    volume = SMA['v']
    date_a = pd.to_datetime(timestamp, unit='s')
    #Stores everything as dictionaries before putting into dataframe
    stock_raw_data = {"Date":date_a,"Open":open_price,"Close":close_price,"High":high_price,"Low":low_price,"Volume":volume}  
    #Makes the dataframe
    stockMA_df = pd.DataFrame(stock_raw_data)
    #Below makes the date into year, month, day
    stockMA_df['Date'] = pd.to_datetime(stockMA_df["Date"]).dt.date  
    # Add ticker symbol to what was pulled
    for index, row in stockMA_df.iterrows():
        stockMA_df.loc[index,"Ticker_Symbol"] = ticker
    #Re orders the data frame
    stockMA_df = stockMA_df.reindex(['Ticker_Symbol','Date','Open','Close','High','Low','Volume'], axis = 1)
    print(stockMA_df)
    stockMA_df.to_sql('Stock_History', engine, if_exists = 'append', index = False)

def Metrics(ticker,api_key):
    metrics = requests.get(f'https://finnhub.io/api/v1/stock/metric?symbol={ticker}&metric=all&token={api_key}').json()

    #This is a part of the metrics function above
    longTermDebt2EquityAnnual = metrics['metric']['longTermDebt/equityAnnual']
    bookValuePerShareAnnual = metrics['metric']['bookValuePerShareAnnual']
    bookValuePerShareQuarterly = metrics['metric']['bookValuePerShareQuarterly']
    cashFlowPerShareTTM = metrics['metric']['cashFlowPerShareTTM']
    freeCashFlowPerShareTTM = metrics['metric']['freeCashFlowPerShareTTM']
    revenuePerShareTTM = metrics['metric']['revenuePerShareTTM']


    #Setting up and building the DataFrame
    raw_metrics = {"Debt/Equity, Annual":longTermDebt2EquityAnnual,"Book Value/Share, Annual":bookValuePerShareAnnual,
                "Book Value/Share, Quarter":bookValuePerShareQuarterly,"CF/Share, TTM":cashFlowPerShareTTM, 
                "Free CF/Share, TTM":freeCashFlowPerShareTTM, "Revenue/Share, TTM":revenuePerShareTTM}
    #raw_metrics
    metrics_key = []
    metrics_value = []

    for key, value in raw_metrics.items():
        metrics_key.append(key)
        metrics_value.append(value)

    Metrics_df = pd.DataFrame({"Ticker_Symbol":ticker,"Data":metrics_key,"Values":metrics_value})
    print(Metrics_df)
    try:
        Metrics_df.to_sql('Stock_Metrics', engine, if_exists = 'append', index = False)
    except:
        print(f"ERROR {ticker} bad data somewhere")

def EPSdata(ticker, api_key,s_yr,s_mo,s_day):
    s_date = str(f'{s_yr}-{s_mo}-{s_day}')

    #This gets the current time
    end_timeseries = round(time())
    date_end = pd.to_datetime(end_timeseries,unit='s')

    #This breaks down to current date
    e_yr = str(date_end)[:4]
    e_mo = str(date_end)[5:7]
    e_day = str(date_end)[8:10]
    e_date = str(f'{e_yr}-{e_mo}-{e_day}')

    EPS = requests.get(f'https://finnhub.io/api/v1/calendar/earnings?from={s_date}&to={e_date}&symbol={ticker}&token={api_key}').json()
    date = []
    year = []
    quarter = []
    epsActual = []
    epsEstimate = []
    revenueActual = []
    revenueEstimate = []
    x=0

    for val in EPS['earningsCalendar']:
        date.append(EPS['earningsCalendar'][x]['date'])
        year.append(EPS['earningsCalendar'][x]['year'])
        quarter.append(EPS['earningsCalendar'][x]['quarter'])
        epsActual.append(EPS['earningsCalendar'][x]['epsActual'])
        epsEstimate.append(EPS['earningsCalendar'][x]['epsEstimate'])
        revenueActual.append(EPS['earningsCalendar'][x]['revenueActual'])
        revenueEstimate.append(EPS['earningsCalendar'][x]['revenueEstimate'])
        x=x+1

    #Setting up and building the DataFrame
    raw_EPS = {"Ticker_Symbol":ticker,"Date":date,"Year":year,"Quarter":quarter, "EPS_Actual":epsActual, 
                "EPS_Estimate":epsEstimate, "Actual_Revenue":revenueActual,"Estimated_Revenue":revenueEstimate}

    EPS_df = pd.DataFrame(raw_EPS)
    # Doing some extra stuff in pandas
    EPS_df['EPS_Diff'] = EPS_df['EPS_Actual']-EPS_df['EPS_Estimate']
    EPS_df['Revenue_Diff'] = EPS_df['Actual_Revenue']-EPS_df['Estimated_Revenue']

    try:
        print(EPS_df.head(5))
        EPS_df.to_sql('Stock_EPS', engine, if_exists = 'append', index = False)
    except:
        print(f"ERROR {ticker} bad data somewhere")

def Description(tSymbol):
    # use yfinance to download the company information
    stock = yf.Ticker(tSymbol)
    # get stock info
    r = stock.info
    # return just the business summary
    longbs = r['longBusinessSummary']
    # Pass the single row data frame item into a single list for the PD dataframe
    tSymbol = [tSymbol]
    longbs = [longbs]
    # pass it to a pandas dataframe
    profile_df = pd.DataFrame({"Ticker_Symbol":tSymbol, "Description":longbs})
    # Upload to SQL
    try:
        print(profile_df.head(5))
        profile_df.to_sql('Stock_Info', engine, if_exists = 'append', index = False)
    except:
        try:
            longbs = ["need to find this data"]
            profile_df = pd.DataFrame({"Ticker_Symbol":tSymbol,"Description":longbs})
            print(profile_df.head(5))
            profile_df.to_sql('Stock_Info', engine, if_exists = 'append', index = False)
        except:
            print(f"ERROR {tSymbol} bad data somewhere")

def Main(ticker):
    api_key = "API HERE"
    #Start date for candles and M.A. data
    start_year = 1980
    start_month = 1
    start_day = 1
    resolution_CS = 'D'
    time_period = 20
    # Resolution for target function
    tech_ind_resolution = 'D'
    #Start date for EPS data
    s_yr = 2010
    s_mo = 1
    s_day = 1
    # Activate Engine
    engine = create_engine('mssql+pymssql://')
    print(ticker)
    try:
        PullStockMovingAvgs(ticker, api_key, time_period, resolution_CS, start_year, start_month, start_day)
    except:
        print(f"{ticker} had an error pulling the stock historical data")
    try:
        Metrics(ticker,api_key)
    except:
        print(f"{ticker} had an error pulling the stock metric data")
    try:
        Peers(ticker,api_key)
    except:
        print(f"{ticker} had an error pulling the stock peers data")
    try:
        Profile(ticker,api_key)
    except:
        print(f"{ticker} had an error pulling the stock profile data")
    try:
        GetTargets(ticker,api_key,tech_ind_resolution)
    except:
        print(f"{ticker} had an error pulling the stock target data")
    try:
        EPSdata(ticker, api_key,s_yr,s_mo,s_day)
    except:
        print(f"{ticker} had an error pulling the EPS data")
    try:
        Description(ticker)
    except:
        print(f"{ticker} had an error pulling the Description data")