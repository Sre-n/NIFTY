import quandl
import os
import pandas as pandas
import pickle
import bs4 as bs4
import requests

quandl.ApiConfig.api_key=''#complete

startdate=""#complete
enddate=""#complete

def nifty_50_list():
    resp=requests.get('')#complete
    soup=bs.BeautifulSoup(resp.txt,'lxml')

    table=soup.find('table',{'class':'wikitable sortable'},'today')
    
    tickers=[]
    for row in table.findAll('tr')[1:]:
        ticker=row.findAll('td')[1].text
        tickers.append(ticker)
    with open("nift50_list.pickle","wb")as f:
        pickle.dump(tickers,f)
    tickers.append('')#complete
    tickers.remove('')#complete
    return tickers

def get_nifty50_list(scrap=False):
    if scrap:
        tickers=nift_50_list()
    else:
        with open("nifty50_list.pickle","rb")as f:
            tickers=pickle.load(f)
    return tickers

def getStockdataFromQuandl(ticker):
    quandl_code="NSE/"+ticker
    try:
        if not os.path.exists(f'stock_data/{ticker}.csv'):
            data=quandl.get(quandl_code,start_date=start_date,end_date=end_date)
            data.to_csv(f'stock_data/{ticker}.csv')
        else:
            print(f"stock data for {ticker} already exists")
    except quandl.errors.quandl_error.NotFoundError as e:
        print(ticker)
        print(str(e))

def load():
    tickers=get_nifty50_list(True)
    df=pd.DataFrame()
    for ticker in tickers:
        getStockdataFromQuandl(ticker)
        try:
            data=pd.read_csv(f'stock_data/{ticker}.csv')
            if(ticker == "NIFTY_50"):
                data.rename(columns={'Close':f"{ticker}_Close",'Shares Traded':f"{ticker}_Volume"},inplace=True)
            else:
                data.rename(columns={'Close':f"{ticker}_Close",'Total Trade Quantity':f"{ticker}_Volume"},inplace=True)
            df=pd.concat([df,data[f'{ticker}_Volume'],data[f'{ticker}_Close']],axis=1)
        except Exception as e:
            print(f"couldn't find {ticker}")
            print(str(e))
    df.to_csv('nifty50_closingprices.csv')
    df.dropna(inplace=True)
    return df
