import os
from tqdm import tqdm
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

output = "Twitter.parquet"

ccs = [cc for cc in os.listdir("../TwitterScraper/data/scraped/") if "." not in cc]

for cc in tqdm(ccs):
    print(cc)
    cc_dict = pd.read_pickle("../TwitterScraper/data/scraped/{}/tweets.pkl".format(cc))
    print(sum([cc_dict[x].shape[0] for x in cc_dict.keys()]))
    for key in cc_dict.keys():
        cc_df = cc_dict[key]
        cc_df["datetime"] = pd.to_datetime(cc_df["created_at"])
        cc_df["date"] = cc_df["datetime"].dt.date
        cc_df["title"] = None
        cc_df["content"] = cc_df["text"]
        cc_df["author"] = cc_df["author_id"]
        cc_df["source"] = "Twitter"
        cc_df["cc"] = cc
        cc_df["metric_like"] = cc_df["public_metrics"].apply(lambda x: x["like_count"])
        cc_df["metric_interaction"] = cc_df["public_metrics"].apply(lambda x: x["retweet_count"] + x["reply_count"] + x["quote_count"])
        cc_df = cc_df[["date", "datetime", "title", "content", "author", "source", "cc", "metric_like", "metric_interaction"]]
        table = pa.Table.from_pandas(cc_df)
        # Write direct to your parquet file
        pq.write_to_dataset(table, root_path=output, partition_cols=['date', 'source', 'cc'])

        # read
        ## dataset = pq.ParquetDataset(output, validate_schema=False, filters=[('cc', '=', '1ST')])
        ## dataset = pq.ParquetDataset(output, validate_schema=False, filters=[('date', '=', '2017-06-06')])
        ## df = dataset.read(columns= ["content"]).to_pandas()