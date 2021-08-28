from alpaca_trade_api.rest import REST, TimeFrame
import os
import pandas as pd
import datetime
from datetime import date

def getAPI():
    
    keyfile = open('alpaca.key','r')
    key = keyfile.readline().replace('\n','')
    secret = keyfile.readline().replace('\n','')
    os.environ['APCA_API_KEY_ID']=key
    os.environ['APCA_API_SECRET_KEY']=secret
    api = REST()
    return api

# Get a list of all tradable assets 
    
def getAssets(api):
    
    assets = api.list_assets(status=None, asset_class=None)
    
    assets_viable = []
    
    for a in assets:
        
        if getattr(a,'class') == 'us_equity' and getattr(a,'fractionable')==True and getattr(a,'status')=='active' and getattr(a,'tradable')==True:
            
            assets_viable.append(getattr(a,'symbol'))
            
    return assets_viable
            
# Format date for input to Alpaca API
    
def formatDate(date):
    
    return date.strftime("%Y-%m-%d")
    
# Get 5-year historical data for an asset
# Approx. 4,000 rows per year, 10,000 limit per API call
# Cost: 5 API calls
            
def getFiveYearHistorical(api,symbol):
    
    today = date.today()
    
    historical_final = pd.DataFrame()
    
    # Add historical data to dataframe year-by-year
    
    for i in range(5):
        
        startyear = 5-i
        endyear = startyear - 1
        
        startdate = datetime.datetime(today.year-startyear,today.month,today.day)
        enddate = datetime.datetime(today.year-endyear,today.month,today.day)
        
        startdate = formatDate(startdate)
        enddate = formatDate(enddate)
    
        historical = api.get_bars(symbol, TimeFrame.Hour, startdate, enddate, limit=10000, adjustment='raw').df
        
        historical = historical.reset_index()
        
        historical_final = pd.concat([historical_final,historical])
        
    historical_final = historical_final.drop_duplicates()
    
    historical_final = historical_final.reset_index(drop=True)
    
    return historical_final