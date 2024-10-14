# https://tahiriamine9.medium.com/python-hdfs-cd822199799e
import time
import datetime
import requests
import pandas as pd
from xgboost import XGBRegressor
import json
from kafka import KafkaProducer
fsym='BTC'
tsym='USDT'
api_key='0bd0855806a03632864100a9243488a8a2f7930748bb853c30dc351dbd371da9'


loaded_model = XGBRegressor()
loaded_model.load_model("xgboost_model.json")

#kafka setting
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def check_error(err, msg):
    if err is  None:
        print(msg)
    else:
        print(f'failed: {err}')

def get_data_from_api(date,exchange,limit: int=1):
    url = f'https://min-api.cryptocompare.com/data/v2/histominute'
    parameter={
        'fsym' : fsym,
        'tsym' : tsym,
        'toTs' : date,
        'limit' : limit,
        'api_key' : api_key,
        'e' : exchange
    }
    response = requests.get(url,parameter)
    return response.json()['Data']['Data']

def get_data_minutes(limit: int = 29):
    times = []
    prices = []
    date = datetime.datetime.now().timestamp()
    one_day_data_per_minute = get_data_from_api(date,'binance',limit)
    for item in one_day_data_per_minute:
        times.append(datetime.datetime.fromtimestamp(item['time'])) 
        prices.append(item['high']) 
    return {'times':times,'prices':prices}

def get_stream_data():
    exchanges = ['coinbase','bybit','binance','bigone','bequant']
    date = datetime.datetime.now().timestamp
    data_out = []
    close_item = []
    for item_exchange in exchanges:
        date = datetime.datetime.now().timestamp()
        one_day_data_per_minute = get_data_from_api(date,item_exchange)
        time_now = str(datetime.datetime.now().time())
        high = one_day_data_per_minute[0]['high']
        low  = one_day_data_per_minute[0]['low']
        close= one_day_data_per_minute[0]['close']
        open = one_day_data_per_minute[0]['open']
        data_out.append( {'high':high,'low':low,'close':close,'open':open,'exch':item_exchange,'time':time_now})
    data_out = sorted(data_out, key=lambda d: d['close'],reverse=True)
    return {'data':data_out}

def make_df_from_data_hist(data):
    return pd.DataFrame([data],columns=[i for i in range(30)])

def predict_one_minute():
    data=get_data_minutes()['prices']
    df=make_df_from_data_hist(data=data)
    y_loaded_pred = loaded_model.predict(df)
    if y_loaded_pred[0] >= data[29]:
       status = True
    else:
       status = False
    print(data[29])
    producer.send('topic_live_data', {'predict-num':str(y_loaded_pred[0]),'predict-status':status,'data-status':'live-predict'})
    producer.flush()

def get_data_five_second():
    data=get_stream_data()
    data['data-status'] = 'live5s'
    topic='topic_live_data'
    print(data)
    print(producer.send(topic,value=data))
    producer.flush()

item=0
while 1:
    #try:
       get_data_five_second()
       time.sleep(5)
       if item == 11:
           predict_one_minute()
           item = 0
       else:
           item += 1
    #except:
    #   print('connection error')
