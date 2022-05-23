import numpy as np
import pandas as pd
import requests
from datetime import datetime

# retrieve data directly from coinranking

#token_ticker = "ROUND"
#token_name = "RoundCoin"
#token_url = "https://coinranking.com/api/v2/coin/haIB5aDV93tAf/history?timePeriod=5y&refe"
token_ticker = "1ST"
token_name = "FirstBlood"
token_url = "https://coinranking.com/api/v2/coin/eqWYmYUirEO2S/history?timePeriod=5y&referenceCurrencyUuid=yhjMzLPhuIDl"

response = requests.get(token_url)
response_dict = response.json()

df = pd.DataFrame(response_dict["data"]["history"])
df["timestamp"] = df["timestamp"].apply(lambda x: datetime.fromtimestamp(int(x)))
df["price"] = df["price"].apply(lambda x: float(x))
df = df.sort_values(by="timestamp")


# prepare data to be saved
def transform_toFinalFormat(row):
    new_row = pd.Series()
    new_row["Date"] = row["timestamp"].strftime("%b %d, %Y")
    new_row["Open*"] = "${:.5f}".format(row["price"])
    new_row["High"] = "${:.5f}".format(row["price"])
    new_row["Low"] = "${:.5f}".format(row["price"])
    new_row["Close**"] = "${:.5f}".format(row["price"])
    new_row["Volume"] = np.nan
    new_row["Market Cap"] = np.nan
    return new_row

df = df.apply(transform_toFinalFormat, axis= 1)

df.to_csv("./data/cc/{}_{}.csv".format(token_ticker, token_name), index= False)