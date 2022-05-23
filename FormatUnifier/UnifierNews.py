import os
from tqdm import tqdm
import numpy as np
import pandas as pd
tqdm.pandas()
import pyarrow as pa
import pyarrow.parquet as pq

def flatten(t):
    return [item for sublist in t for item in sublist]

def check_cc_mentioned(text: str, cc_dict: dict):
    text = text.lower()
    ccs_in_text = list()
    for key in cc_dict.keys():
        for alt in cc_dict[key]:
            if alt in text:
                ccs_in_text.append(key)
    return ccs_in_text


output = "News.parquet"

#blocks = [block for block in os.listdir("../NewsScraper/data") if (".csv" in block) and ("_full_text" in block)]
blocks = [block for block in os.listdir("../NewsScraper/data") if (".csv" in block)]
ccs = pd.read_excel("../CCScraper/data/cc_master.xlsx", index_col= 0)
ccs["all"] = ccs.apply(lambda x: list(set([x for x in (str(x["Name"]).lower() + ", " + str(x["Symbol"]).lower() + ", " + str(x["Alt Nam"]).lower() + ", " + str(x["Alt Symbol"]).lower()).split(", ") if not x == "nan"])), axis= 1).to_list()
cc_dict = dict(zip(ccs["Symbol"], ccs["all"]))

for block in blocks:
    print(block)

    articles = pd.read_csv("../NewsScraper/data/{}".format(block))

    articles["cc"] = articles.progress_apply(lambda x: check_cc_mentioned(x["header"] + x["text"], cc_dict), axis= 1)

    counts = pd.Series(flatten(articles["cc"].to_list())).value_counts()
    print(counts)

    articles = articles.explode("cc")

    cc_df = pd.DataFrame()
    cc_df["datetime"] = pd.to_datetime(articles["date"])
    cc_df["date"] = cc_df["datetime"].dt.date
    cc_df["title"] = articles["header"]
    #cc_df["content"] = articles["header"] + " " + articles["text"]
    cc_df["content"] = articles["header"]
    cc_df["author"] = articles["author"].apply(lambda x: x.replace("by ", ""))
    cc_df["source"] = "CoinTelegraph"
    cc_df["cc"] = articles["cc"]
    #cc_df["metric_like"] = articles["shares"]
    cc_df["metric_like"] = np.nan
    cc_df["metric_interaction"] = articles["viewCount"]
    cc_df = cc_df[["date", "datetime", "title", "content", "author", "source", "cc", "metric_like", "metric_interaction"]]
    table = pa.Table.from_pandas(cc_df)
    # Write direct to your parquet file
    pq.write_to_dataset(table, root_path=output, partition_cols=['date', 'source', 'cc'])

    # read
    ## dataset = pq.ParquetDataset(output, validate_schema=False, filters=[('cc', '=', '1ST')])
    ## dataset = pq.ParquetDataset(output, validate_schema=False, filters=[('date', '=', '2017-06-06')])
    ## df = dataset.read(columns= ["content"]).to_pandas()