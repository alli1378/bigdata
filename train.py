
# xgboost
import findspark 
findspark.init()
from pyspark.sql import SparkSession
from sklearn.model_selection import train_test_split
import xgboost
import numpy as np
import pandas as pd
spark = SparkSession.builder.appName("LogisticRegression with PySpark MLlib").getOrCreate()

df = spark.read.csv('hdfs://master:50000/data/BTCUSDTminute.csv', header=True, inferSchema=True)
df = df.toPandas()
df["Date"] = pd.to_datetime(df["Date"])
df = df.iloc[::-1]
df=df.drop(['Unix','Symbol','Open','High','Low','Volume BTC','Volume USDT','tradecount'],axis=1)

def create_features_and_labels(df, window_size=30):
    features = []
    labels = []
    # print(len(df)//30)
    for i in range(len(df)//30):
            if i==0: 
                temp=i
                # print(temp,i)
            else:
                temp=i*30
            window = df['Close'].iloc[temp:temp + window_size ].values
            features.append(window)
            label = df['Close'].iloc[temp + window_size]
            labels.append(label)
    return pd.DataFrame(features) , pd.Series(labels)

# Generate the features
window_size = 30
X,y = create_features_and_labels(df, window_size)
#print(y[4313])
#print(X[29][4313],X[28][4313],X[27][4313],X[26][4313],X[25][4313])

# Display the resulting DataFrame
# print(y)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
# # ایجاد مدل XGBoost و آموزش آن
model = xgboost.XGBRegressor(n_estimators=100, learning_rate=0.1, max_depth=5, random_state=42)
model.fit(X_train, y_train)
#print(train)
# # پیش‌بینی و ارزیابی مدل
y_pred = model.predict(X_test)
#mse = mean_squared_error(y_test, y_pred)
#print(f"Mean Squared Error: {mse}")
# print(accuy_test,y_pred))
score = model.score(X_test, y_test) # test score
# نمایش نتایج پیش‌بینی
results = pd.DataFrame({'Actual': y_test, 'Predicted': y_pred})
print(score)
print(results)
#from sklearn.metrics import mean_absolute_error

# محاسبه MAE
#mae = mean_absolute_error(y_test, y_pred)
#print(f"Mean Absolute Error: {mae}")
#print(model.get_booster().get_score())
#model.save_model("xgboost_model.json")
# محاسبه RMSE
# rmse = mean_squared_error(y_test, y_pred, squared=False)
# print(f"Root Mean Squared Error: {rmse}")
