import dbmanager
import pandas as pd
import numpy as np
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import precision_score
import tqdm


'''

Dataset evaluation:
    
    - AUC over past 3 months
    
Dataset parameters to test: 
    
    - Stock symbol: All (Use 5-10 randomly selected per experiment session to identify common trends)
    - Time sample size: 1 Hour - 7 Hours
    - Length of historical data used as training data: Last 5 Years, Last 2.5 Years, Last Year, Last 6 Months
    - # of Memory Vectors: 0 - 10
    
'''

def makeDataset(symbol,sample,historical_length,memory,memory_features):
    
    historical = dbmanager.getHistorical(symbol)
    historical['timestamp'] = pd.to_datetime(historical['timestamp'])
    historical = historical.set_index('timestamp',drop=True)
    
    # Hour of the day
    historical['hour'] = historical.index.hour
    # Day of the week
    historical['weekday'] = historical.index.weekday
    
    if sample != 1:
        sample_formatted = '{}H'.format(sample)
        historical = historical.resample(sample_formatted).agg('first')
    
    # Later: Implement trimming down training data size
    
    if historical_length != '5Y':
        
        historical = historical.last(historical_length)
        
    if memory != 0:
        
        for m in range(1, memory+1):
            
            for f in memory_features:
                
                mem = historical[f].shift(m)
                
                historical[str(f)+str(m)] = mem
        
    # Create target vector: % change in price over next interval
        
    historical['Future_Price_Change'] = historical['close'].diff(-1)
    historical['Future_Price_Change'] = np.sign(historical['Future_Price_Change'])
    historical['Future_Price_Change'] = historical['Future_Price_Change'].replace(-1,0)
   
    historical = historical.dropna()
    
    return historical
    
    
def evaluateModel(symbol,sample=1,historical_length='6M',memory=0,memory_features=['close','volume'],model=GradientBoostingClassifier(),evalperiod='1M'):
    
    dataset = makeDataset(symbol,sample,historical_length,memory,memory_features)
    
    # Test set: Last month
    
    test = dataset.last(evalperiod).reset_index(drop=True)
    
    # Training set: All prior data
    
    train = dataset[:len(dataset)-len(test)].reset_index(drop=True)
    
    X_Train = train.drop(columns = ['Future_Price_Change'])
    Y_Train = train['Future_Price_Change']
    
    X_Test = test.drop(columns = ['Future_Price_Change'])
    Y_Test = test['Future_Price_Change']
    
    model.fit(X_Train,Y_Train)
    
    pred = model.predict(X_Test)
    pred_prob = model.predict_proba(X_Test)[:,1]
    
    eval_df = pd.DataFrame()
    eval_df['pred'] = pred
    eval_df['pred_prob'] = pred_prob
    eval_df['target'] = Y_Test
    
    # Model precision
    precision = round(precision_score(eval_df['target'],eval_df['pred']),3)
    
    results = {'symbol':symbol,
               'sample':sample,
               'historical_length':historical_length,
               'memory':memory,
               'memory_features':memory_features,
               'model':type(model).__name__,
               'eval_period':eval_period,
               'eval_n':len(eval_df),
               'train_n':len(X_Train),
               'precision':precision}

    return results

# Test all assets using default model evaluation parameters

def testDefault():
    
    assets = dbmanager.getAssets()
    
    for i in tqdm.tqdm(range(len(assets)),desc='Testing all asset models using default params'):
        
        symbol = assets.iloc[i].asset
        last_updated = assets.iloc[i].last_updated
        
        results = evaluateModel(a)
        results['last_updated'] = last_updated
        
        dbmanager.recordPerformance(results)
        
# Test all assets over a range of parameters. If a constant value is entered, holds this value constant during tests. Otherwise, uses default params.
        
def testRange(sample=[],historical_length=[],memory=[],memory_features=[],model=[],evalperiod=[]):
    
    print('placeholder')