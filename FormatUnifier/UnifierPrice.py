import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

output = "Price.parquet"

price_df = pd.read_csv("../AnalysisCCCorrelations/data/2017_top50/price_data.csv", index_col=0)
price_df = price_df.rename({"strax": "strat"}, axis= 1)
price_df = pd.melt(price_df.reset_index(), id_vars= ["index"])
price_df.rename({"index": "datetime", "value": "price", "variable": "cc"}, axis= 1, inplace= True)
volume_df = pd.read_csv("../AnalysisCCCorrelations/data/2017_top50/volume_data.csv", index_col=0)
volume_df = volume_df.rename({"strax": "strat"}, axis= 1)
volume_df = pd.melt(volume_df.reset_index(), id_vars= ["index"])
volume_df.rename({"index": "datetime", "value": "volume", "variable": "cc"}, axis= 1, inplace= True)

df = pd.merge(price_df, volume_df, left_on= ["datetime", "cc"], right_on= ["datetime", "cc"])
df["datetime"] = pd.to_datetime(df["datetime"])
df["date"] = df["datetime"].dt.date
df["source"] = "coinMarketCap"
df = df[["date", "datetime", "price", "volume", "source",  "cc"]]

table = pa.Table.from_pandas(df)
# Write direct to your parquet file
pq.write_to_dataset(table, root_path=output, partition_cols=['date', 'cc'])





#cc_df = pd.DataFrame()
#cc_df["datetime"] = pd.to_datetime(articles["date"])
#cc_df["date"] = cc_df["datetime"].dt.date
#cc_df["title"] = articles["header"]
#cc_df["content"] = articles["text"]
#cc_df["author"] = articles["author"].apply(lambda x: x.replace("by ", ""))
#cc_df["source"] = "CoinTelegraph"
#cc_df["cc"] = articles["cc"]
#cc_df["metric_like"] = articles["shares"]
#cc_df["metric_interaction"] = articles["viewCount"]
#cc_df = cc_df[["date", "datetime", "title", "content", "author", "source", "cc", "metric_like", "metric_interaction"]]
#table = pa.Table.from_pandas(cc_df)
## Write direct to your parquet file
#pq.write_to_dataset(table, root_path=output, partition_cols=['date', 'source', 'cc'])