import os
import ast
from tqdm import tqdm
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

base_path = "../RedditScraper[not used]/data/scraped"
output = "Twitter.parquet"

subs = [sub for sub in os.listdir("../RedditScraper[not used]/data/scraped/") if "." not in sub]
ccs = pd.read_excel("../RedditScraper/data/cc_survival_2017_top50_redditLinks.xlsx", index_col= 0)
ccs["subreddits"] = ccs["True Reddit Links"].apply(lambda xl: [x.split("/")[-1] for x in ast.literal_eval(xl)])
subs_dict = dict(zip(ccs["Symbol"], ccs["subreddits"]))

for cc in tqdm(subs_dict.keys()):
    cc_df = pd.DataFrame()
    for sub in subs_dict[cc]:
        item_list = pd.read_csv(base_path + "/{}/submissions.csv".format(sub))
        for item in item_list["ID"].to_list():
            item_df = pd.read_pickle(base_path + "/{}/{}.pkl".format(sub, item))
            item_df["num_replies"] = item_df.shape[0]-1
            # filter for posts only
            item_df = item_df.loc[item_df["isPost"] == "True"]
            cc_df = cc_df.append(item_df)

    cc_df["datetime"] = pd.to_datetime(cc_df["created_utc"], unit= "s")
    cc_df["date"] = cc_df["datetime"].dt.date
    cc_df["title"] = None
    cc_df["content"] = cc_df["text"]
    cc_df["author"] = cc_df["author"]
    cc_df["source"] = "Reddit"
    cc_df["cc"] = cc
    cc_df["metric_like"] = cc_df.apply(lambda x: int(x["ups"]) - int(x["downs"]) if (int(x["ups"]) - int(x["downs"])) > 0 else 0, axis=1)
    cc_df["metric_interaction"] = cc_df["num_replies"]
    cc_df = cc_df[["date", "datetime", "title", "content", "author", "source", "cc", "metric_like", "metric_interaction"]]
    table = pa.Table.from_pandas(cc_df)
    # Write direct to your parquet file
    pq.write_to_dataset(table, root_path=output, partition_cols=['date', 'source', 'cc'])

    # read
    ## dataset = pq.ParquetDataset(output, validate_schema=False, filters=[('cc', '=', '1ST')])
    ## dataset = pq.ParquetDataset(output, validate_schema=False, filters=[('date', '=', '2017-06-06')])
    ## df = dataset.read(columns= ["content"]).to_pandas()