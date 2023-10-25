import pymongo
import pandas as pd


#!pip3 install pandas smartapi-python websocket-client pyotp login
#!pip --no-cache-dir install --upgrade smartapi-python

from SmartApi import SmartConnect as sc
import time as tt
import requests  as re
import pandas as pd
from datetime import datetime,date,time
import datetime as dt
import login as l

pd.set_option('display.max_columns', None)  # To display all columns without truncation
# pd.set_option('max_columns', None)


def initSymTkMap():
    url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
    d  = re.get(url).json()
    global tk_df
    tk_df = pd.DataFrame.from_dict(d)
    tk_df['expiry'] = pd.to_datetime(tk_df['expiry'])
    tk_df = tk_df.astype({'strike': float})
    l.token_map = tk_df

expiri = "2023-10-26"

def getTkInfo(exch_seg, instrumenttype, symbol, strike_price, pe_ce):
    df = l.token_map
    strike_price = strike_price*100

    if exch_seg == 'NFO' and (instrumenttype == 'OPTSTK' or  instrumenttype== 'OPTIDX'):
        return df[(df['exch_seg'] == 'NFO') & (df['instrumenttype'] == instrumenttype) & (df['name'] == symbol) & (df['strike'] == strike_price) & (df['symbol'].str.endswith(pe_ce) & (df['expiry']==  expiri))].sort_values(by ='symbol')

#Folds

fromdate = f'2023-10-25 09:15'
todate = f'2023-10-25 15:30'


#m01
def getCandleDatam01(symbolInfo):

    try:
        historicParam={
        "exchange": symbolInfo.exch_seg,
        "symboltoken": symbolInfo.token,
        "interval": "ONE_MINUTE",
        "fromdate": fromdate ,
        "todate": todate
        }
        res_json=  obj.getCandleData(historicParam)
        columns = ['timestamp','open','high','low','close','volume']
        df = pd.DataFrame(res_json['data'], columns=columns)
        df['timestamp'] = pd.to_datetime(df['timestamp'],format = '%Y-%m-%dT%H:%M:%S')
        df['symbol'] = symbolInfo.symbol
        df['expiry'] = symbolInfo.expiry
        print(f"Done for {symbolInfo.symbol}")
        tt.sleep(0.2)
        return df
    except Exception as e:
        print(f"Historic Api failed: {e} {symbolInfo.symbol}")


#m03
def getCandleDatam03(symbolInfo):

    try:
        historicParam={
        "exchange": symbolInfo.exch_seg,
        "symboltoken": symbolInfo.token,
        "interval": "THREE_MINUTE",
        "fromdate": fromdate ,
        "todate": todate
        }
        res_json=  obj.getCandleData(historicParam)
        columns = ['timestamp','open','high','low','close','volume']
        df = pd.DataFrame(res_json['data'], columns=columns)
        df['timestamp'] = pd.to_datetime(df['timestamp'],format = '%Y-%m-%dT%H:%M:%S')
        df['symbol'] = symbolInfo.symbol
        df['expiry'] = symbolInfo.expiry
        print(f"Done for {symbolInfo.symbol}")
        tt.sleep(0.2)
        return df
    except Exception as e:
        print(f"Historic Api failed: {e} {symbolInfo.symbol}")



#m15
def getCandleDatam15(symbolInfo):

    try:
        historicParam={
        "exchange": symbolInfo.exch_seg,
        "symboltoken": symbolInfo.token,
        "interval": "FIFTEEN_MINUTE",
        "fromdate": fromdate ,
        "todate": todate
        }
        res_json=  obj.getCandleData(historicParam)
        columns = ['timestamp','open','high','low','close','volume']
        df = pd.DataFrame(res_json['data'], columns=columns)
        df['timestamp'] = pd.to_datetime(df['timestamp'],format = '%Y-%m-%dT%H:%M:%S')
        df['symbol'] = symbolInfo.symbol
        df['expiry'] = symbolInfo.expiry
        print(f"Done for {symbolInfo.symbol}")
        tt.sleep(0.2)
        return df
    except Exception as e:
        print(f"Historic Api failed: {e} {symbolInfo.symbol}")


#m30
def getCandleDatam30(symbolInfo):

    try:
        historicParam={
        "exchange": symbolInfo.exch_seg,
        "symboltoken": symbolInfo.token,
        "interval": "THIRTY_MINUTE",
        "fromdate": fromdate ,
        "todate": todate
        }
        res_json=  obj.getCandleData(historicParam)
        columns = ['timestamp','open','high','low','close','volume']
        df = pd.DataFrame(res_json['data'], columns=columns)
        df['timestamp'] = pd.to_datetime(df['timestamp'],format = '%Y-%m-%dT%H:%M:%S')
        df['symbol'] = symbolInfo.symbol
        df['expiry'] = symbolInfo.expiry
        print(f"Done for {symbolInfo.symbol}")
        tt.sleep(0.2)
        return df
    except Exception as e:
        print(f"Historic Api failed: {e} {symbolInfo.symbol}")


#H1
def getCandleDataH(symbolInfo):

    try:
        historicParam={
        "exchange": symbolInfo.exch_seg,
        "symboltoken": symbolInfo.token,
        "interval": "ONE_HOUR",
        "fromdate": fromdate ,
        "todate": todate
        }
        res_json=  obj.getCandleData(historicParam)
        columns = ['timestamp','open','high','low','close','volume']
        df = pd.DataFrame(res_json['data'], columns=columns)
        df['timestamp'] = pd.to_datetime(df['timestamp'],format = '%Y-%m-%dT%H:%M:%S')
        df['symbol'] = symbolInfo.symbol
        df['expiry'] = symbolInfo.expiry
        print(f"Done for {symbolInfo.symbol}")
        tt.sleep(0.2)
        return df
    except Exception as e:
        print(f"Historic Api failed: {e} {symbolInfo.symbol}")




import pyotp

apikey = 'nBgBs7Ku'
username = 'P547740'
pwd = '8894'
token = 'T2PX34ABOGB3VZIVENMUAOJ5PQ'



from SmartApi import SmartConnect

obj = SmartConnect(api_key=apikey)
data = obj.generateSession(username,pwd,pyotp.TOTP(token).now())

refreshToken= data['data']['refreshToken']


feedToken = obj.getfeedToken()
l.feed_token = feedToken
initSymTkMap()



#######################
print("done")


#NIFTY26OCT2319350PE    

tokenInfo = getTkInfo('NFO','OPTIDX','NIFTY', 19350, 'PE').iloc[0]
print(tokenInfo)
symbol = tokenInfo['symbol']
#token = 67295
tokenInfo['token']


lot = int(tokenInfo['lotsize'])
print(symbol, token, lot)

#m01
xdf11 = getCandleDatam01(tokenInfo)
filename11 = "m01_26OCT2319350PE"
xdf11.to_csv(filename11, index=False)

#m03
xdf12 = getCandleDatam03(tokenInfo)
filename12 = "m03_26OCT2319350PE"
xdf12.to_csv(filename12, index=False)

#m15
xdf13 = getCandleDatam15(tokenInfo)
filename13 = "m15_26OCT2319350PE"
xdf13.to_csv(filename13, index=False)

#m30
xdf14 = getCandleDatam30(tokenInfo)
filename14 = "m30_26OCT2319350PE"
xdf14.to_csv(filename14, index=False)

#H1
xdf15 = getCandleDataH(tokenInfo)
filename15 = "H26_OCT2319350PE"
xdf15.to_csv(filename15, index=False)




#NIFTY26OCT2319350CE

tokenInfo = getTkInfo('NFO','OPTIDX','NIFTY', 19350, 'CE').iloc[0]
print(tokenInfo)
symbol = tokenInfo['symbol']
#token = 67295
tokenInfo['token']


lot = int(tokenInfo['lotsize'])
print(symbol, token, lot)

#m01
xdf11 = getCandleDatam01(tokenInfo)
filename11 = "m01_26OCT2319350CE"
xdf11.to_csv(filename11, index=False)

#m03
xdf12 = getCandleDatam03(tokenInfo)
filename12 = "m03_26OCT2319350CE"
xdf12.to_csv(filename12, index=False)

#m15
xdf13 = getCandleDatam15(tokenInfo)
filename13 = "m15_26OCT2319350CE"
xdf13.to_csv(filename13, index=False)

#m30
xdf14 = getCandleDatam30(tokenInfo)
filename14 = "m30_26OCT2319350CE"
xdf14.to_csv(filename14, index=False)

#H1
xdf15 = getCandleDataH(tokenInfo)
filename15 = "H26_OCT2319350CE"
xdf15.to_csv(filename15, index=False)





# Set up the MongoDB client
uri = "mongodb+srv://usr:passwd@clusterx.qpk1zm7.mongodb.net/?retryWrites=true&w=majority"
dbn = "testdb"
cn = "testcol"
client = pymongo.MongoClient(uri)
db = client[dbn]
collection = db[cn]
import pymongo
import os

def upload_csv_to_mongodb(filename, collection_name, database_name):
    # Set up the MongoDB client
    client = pymongo.MongoClient(uri)
    db = client[dbn]
    collection = db[cn]

    # Read the CSV file into a DataFrame
    df = pd.read_csv(filename)

    # Convert DataFrame to a list of dictionaries
    data_dict = df.to_dict(orient="records")

    # Insert data into the MongoDB collection
    collection.insert_many(data_dict)

    # Close the MongoDB connection
    client.close()

# Define your MongoDB collection name and database name
collection_name = "your_collection_name"
database_name = "your_db_name"

# List of CSV files to upload
csv_files = [
    "m01_26OCT2319350PE",
    "m03_26OCT2319350PE",
    "m15_26OCT2319350PE",
    "m30_26OCT2319350PE",
    "H26_OCT2319350PE",
    "m01_26OCT2319350CE",
    "m03_26OCT2319350CE",
    "m15_26OCT2319350CE",
    "m30_26OCT2319350CE",
    "H26_OCT2319350CE",
]

for csv_file in csv_files:
    upload_csv_to_mongodb(csv_file, collection_name, database_name)
