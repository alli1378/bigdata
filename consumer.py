from kafka import KafkaConsumer
import json
from websocket import create_connection
import findspark 
import pandas as pd
findspark.init()
from pyspark.sql import SparkSession
from datetime import datetime
spark = SparkSession.builder.appName("LogisticRegression with PySpark MLlib").getOrCreate()
df = spark.read.csv('hdfs://master:50000/data/data_save_5s.csv', header=True, inferSchema=True)
pandasDF = df.toPandas()
df=pandasDF
#consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'],
#                         auto_offset_reset='latest',
#                         group_id='test',
#                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

#consumer.subscribe(['topic_live_data'])
consumer = KafkaConsumer(
    'topic_live_data', # topic
     group_id=None,
     bootstrap_servers=['localhost:9092'], # bootstrap server
     auto_offset_reset='latest',
     enable_auto_commit=True,
     value_deserializer=lambda x: json.loads(x.decode('utf-8')))
def save_data_5s_in_hdfs(data,df):
    date=datetime.now()
    pre_df={
    'exch':[],'Symbol':[],'Date':[],'open':[],'High':[],'Low':[],'Close':[]
    }
    for data_item in data['data']:
        pre_df['exch'].append(data_item['exch'])
        pre_df['Symbol'].append('BTC/USDT')
        pre_df['Date'].append(date)
        pre_df['open'].append(data_item['open'])
        pre_df['High'].append(data_item['high'])
        pre_df['Low'].append(data_item['low'])
        pre_df['Close'].append(data_item['close'])

        #df.loc[len(df.index)] = [data_item['exch'],'BTC/USDT',date,data_item['open'],data_item['high'],data_item['low'],data_item['close']] 
    #print('asdfghjkl;asdfghj',pre_df)
    pre_df=pd.DataFrame(pre_df)
    #df=df.append(pre_df)
    #print(pre)
    df=pd.concat([df, pre_df])
    print(df)
    sparkDF = spark.createDataFrame(df) 
    sparkDF.write.mode('overwrite').csv("hdfs://master:50000/data/data_save_5s.csv")
    return df

def stream_websocket_data(data):
    ws = create_connection("ws://localhost:4001")
    ws.send(json.dumps(data))
    ws.close()


for message in consumer:
    print(message.value)
    if message.value['data-status'] == 'live5s':
       df=save_data_5s_in_hdfs(message.value,df)
       stream_websocket_data(message.value)
       
    else:
       print(stream_websocket_data(message.value))
consumer.close()
