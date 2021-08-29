import sqlite3
import alpaca
import tqdm
import pandas as pd
from datetime import date

# Gets connection to Historical DB. If DB does not exist, greates DB.

def getConn():
    
    conn = sqlite3.connect('llama.db')
    return conn

# Populates DB with historical data. If DB already contains data, updates it.

def populateHistorical():
    
    conn = getConn()
    cursor = conn.cursor()

    api = alpaca.getAPI()
    
    assets = alpaca.getAssets(api)
    
    today = str(date.today())
    
    # Create table to store asset list
    
    cursor.execute('CREATE TABLE IF NOT EXISTS Assets (asset nvarchar(50), last_updated nvarchar(50))')
    cursor.execute('DELETE FROM Assets;')
        
    for i in tqdm.tqdm(range(len(assets)),desc='Retrieving 5-year historical data'):
        
        try: 
        
            historical = alpaca.getFiveYearHistorical(api,assets[i])
            
            cursor.execute('INSERT INTO Assets (asset,last_updated) VALUES (?,?)',[assets[i],today])
        
            cursor.execute('CREATE TABLE IF NOT EXISTS {} (timestamp nvarchar(50), open real, high real, low real, close real, volume int, trade_count int, vwap real)'.format(assets[i]))
            
            # Delete all rows in table if any previously existed
            
            cursor.execute('DELETE FROM {};'.format(assets[i]));
            
            for t in historical.itertuples():
            
                cursor.execute('INSERT INTO {} (timestamp, open, high, low, close, volume, trade_count, vwap) VALUES (?,?,?,?,?,?,?,?)'.format(assets[i]),[str(t.timestamp),t.open,t.high,t.low,t.close,t.volume,t.trade_count,t.vwap])
                
        except:
                        
            pass
          
    conn.commit()
    conn.close()
    
# Validate that all assets in DB are tradable.

def validateAssets():
    
    conn = sqlite3.connect('llama.db')
    
    cursor = conn.cursor()
    
    dbAssets = pd.read_sql('SELECT * FROM Assets',conn)
    
    api = alpaca.getAPI()    
    tradable = alpaca.getAssets(api)
    
    for i in tdqm.tdqm(range(len(dbAssets)),desc='Validating asset tradability'):
        
        if dbAssets.iloc[i]['asset'] not in tradable:
            
            print('Discovered non-tradable asset: {}'.format(dbAssets.iloc[i]['asset']))
            print('Deleting from database.')
            
            cursor.execute('DELETE FROM Assets WHERE asset = {};'.format(dbAssets.iloc[i]['asset']))
            
            cursor.execute('DROP TABLE IF EXISTS {};'.format(dbAssets.iloc[i]['asset']))
            
    conn.commit()
    conn.close()
    
    print('All assets in database are tradable.')
    
# Get a list of assets from the DB.
    
def getAssets():
    
    conn = sqlite3.connect('llama.db')
    
    assets = pd.read_sql('SELECT * FROM Assets', conn)
    
    conn.close()
    
    return assets

# Retrieves historical data for an asset from the DB.
    
def getHistorical(symbol):
    
    conn = getConn()
    
    historical = pd.read_sql('SELECT * FROM {}'.format(symbol),conn)
    
    conn.close()
    
    return historical

def recordPerformance(results):
    
    conn = getConn()
    cursor = conn.cursor()
    
    cursor.execute('CREATE TABLE IF NOT EXISTS Model_Performance (symbol nvarchar(50), sample int, historical_Length nvarchar(50), memory int, memory_features nvarchar(50), eval_period nvarchar(50), eval_n int, train_n int, precision real)')
    
    memory_features = ''
    
    for f in results['memory_features']:
        
        if memory_features == '':
            memory_features = memory_features + f
        else:
            memory_features = memory_features + '|' + f
            
    cursor.execute('INSERT INTO Model_Performance (symbol, sample, historical_length, memory, memory_features,eval_period, eval_n, train_n, precision) VALUES (?,?,?,?,?,?,?,?,?)',
                   [results['symbol'],results['sample'],results['historical_length'],results['memory'],memory_features,results['eval_period'],results['eval_n'],results['train_n'],results['precision']])
    
    conn.commit()
    conn.close()