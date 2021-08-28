import sqlite3
import alpaca
import tqdm


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
    
    # Create table to store asset list
    
    cursor.execute('CREATE TABLE IF NOT EXISTS Assets (asset nvarchar(50))')
    cursor.execute('DELETE FROM Assets;')
                   
    for a in assets:
        cursor.execute('INSERT INTO Assets (asset) VALUES (?)',[a])
        
    for i in tqdm.tqdm(range(len(assets)),desc='retrieving 5-year historical data.'):
        
        try: 
        
            historical = alpaca.getFiveYearHistorical(api,assets[i])
        
            cursor.execute('CREATE TABLE IF NOT EXISTS {} (timestamp nvarchar(50), open real, high real, low real, close real, volume int, trade_count int, vwap real)'.format(assets[i]))
            
            # Delete all rows in table if any previously existed
            
            cursor.execute('DELETE FROM {};'.format(assets[i]));
            
            for t in historical.itertuples():
            
                cursor.execute('INSERT INTO {} (timestamp, open, high, low, close, volume, trade_count, vwap) VALUES (?,?,?,?,?,?,?,?)'.format(assets[i]),[str(t.timestamp),t.open,t.high,t.low,t.close,t.volume,t.trade_count,t.vwap])
                
        except:
            
            pass
          
    conn.commit()
    conn.close()
    

    
#def validateAssets():
    
    

    
        
        
        


    
    
    
    
    
    