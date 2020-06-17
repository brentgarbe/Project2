#!pip install requirements.txt
from flask import Flask, render_template, jsonify, url_for
import requests
import json
from pathlib import Path
from sqlalchemy import *
from bs4 import BeautifulSoup
import datetime
import time
import yfinance as yf
import pandas as pd
import pymssql
# This is the python webscrape code
import web_scrape


application = Flask("__main__")
engine = create_engine('mssql+pymssql://')

@application.route('/')
def index():
    return render_template('index.html')

@application.route('/api/<ticker>', methods = ["GET"])
def api(ticker):
    if ticker == "undefined":
        return("User needs to type a stock in")
    else:
        df_read_stocks = pd.read_sql_table('Stock_History',engine)
        df_read_stocks = df_read_stocks.loc[(df_read_stocks["Ticker_Symbol"] == ticker), :]
        if df_read_stocks.empty == True:
            web_scrape.Main(ticker)
            time.sleep(60)
            df_read_stocks = pd.read_sql_table('Stock_History',engine)
            df_read_stocks = df_read_stocks.loc[(df_read_stocks["Ticker_Symbol"] == ticker), :]
        # Add date time in different format
        for index, row in df_read_stocks.iterrows():
            df_read_stocks.loc[index, "Date_Time"] = df_read_stocks.loc[index, "Date"].strftime("%m-%d-%Y")
        # df_read_stocks = df_read_stocks.sort_values(by="Date", ascending=True)
        data_stock = df_read_stocks.to_dict(orient="records")
        return jsonify(data_stock)

@application.route('/api1/<ticker>', methods = ["GET"])
def api1(ticker):
    # if ticker is None:
    #     print("User needs to type a stock in")
    # else:
    df_read_stocks_info = pd.read_sql_table('Stock_Info',engine)
    df_read_stocks_info = df_read_stocks_info.loc[(df_read_stocks_info["Ticker_Symbol"] == ticker), :]
    data_stock_info = df_read_stocks_info.to_dict(orient="records")
    return jsonify(data_stock_info)

@application.route('/api2/<ticker>', methods = ["GET"])
def api2(ticker):
    if ticker == "undefined":
        return("User needs to type a stock in")
    else:
        df_read_stocks = pd.read_sql_table('Stock_History',engine)
        df_read_stocks = df_read_stocks.loc[(df_read_stocks["Ticker_Symbol"] == ticker), :]
        for index, row in df_read_stocks.iterrows():
            df_read_stocks.loc[index, "Date_Time"] = df_read_stocks.loc[index, "Date"].strftime("%m-%d-%Y")
        data_stock = df_read_stocks.to_dict(orient="list")
        return jsonify(data_stock)

@application.route('/api3/<ticker>', methods = ["GET"])
def api3(ticker):
    if ticker == "undefined":
        return("User needs to type a stock in")
    else:
        df_read_stocks = pd.read_sql_table('Stock_EPS',engine)
        df_read_stocks = df_read_stocks.loc[(df_read_stocks["Ticker_Symbol"] == ticker), :]
        # df_read_stocks = df_read_stocks.sort_values(by="Date", ascending=True)
        data_stock = df_read_stocks.to_dict(orient="records")
        return jsonify(data_stock)


if __name__ == "__main__":
    application.run(debug=True)

