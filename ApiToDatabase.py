import requests
import pandas as pd 
import sqlalchemy 



#url = "https://api.coindesk.com/v1/bpi/currentprice.json"  
url = "https://api.coincap.io/v2/assets"
header={"content_type":"application/json","Accept_Encoding":"deflate"}
response = requests.get(url,headers=header)
print(response)

responseData=response.json()
#print(responseData)

#df=pandas.json_normalize(responseData) 
#print(df)
#--------------convert categorical data into numerical data----------------------
df = pd.json_normalize(responseData, "data")
df.head()
df.info()

def convert_columns_to_datatypes(df, column_datatype):
    for column, datatype in column_datatypes.items():
        if column in df.columns:
            df[column] = df[column].astype(datatype)
    return df     
column_datatypes = {
    'rank': int,
    'supply': float,
    'maxSupply': float,
    'marketCapUsd': float,
    'volumeUsd24Hr': float,
    'priceUsd': float,
    'changePercent24Hr': float,
    'vwap24Hr': float
}

data = convert_columns_to_datatypes(df, column_datatypes)
data.info()
#--------------missing data-----------------------------------------------------
data["maxSupply"] = data["maxSupply"].fillna(0)
data["explorer"] = data["explorer"].fillna('not available')
data.info()

#---------------round nember to 2 decimal places---------------------------------------------------------------
def round_to_two_decimal_places(number):
    return round(number, 2)
selected_columns = ['supply', 'maxSupply', 'marketCapUsd', 'priceUsd', 'changePercent24Hr', 'vwap24Hr']
data[selected_columns] = data[selected_columns].applymap(round_to_two_decimal_places)
data.head()
#--------------------------------------------------------
engine = sqlalchemy.create_engine("mssql+pyodbc://DESKTOP-QI5KD3C/sampleDB?driver=ODBC+Driver+17+for+SQL+Server")
df.to_sql(name="test5",con=engine,index=False,if_exists="fail")