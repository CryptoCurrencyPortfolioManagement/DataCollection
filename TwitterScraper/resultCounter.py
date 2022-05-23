import os
import numpy as np
import pandas as pd
from tqdm import tqdm
import random
import datetime
from searchtweets import ResultStream, gen_request_parameters, load_credentials, collect_results
import time

# script to extract the number of results which would be retrieved by running the different queries against Twitters API
df = pd.read_excel("./data/cc_survival_2017_top50_twitter.xlsx")
cc_dict_1 = dict(zip(df["Symbol"], df["Name"]))
cc_dict_2 = dict(zip(df.loc[~df["Alt Symbol"].isnull()]["Alt Symbol"], df.loc[~df["Alt Symbol"].isnull()]["Alt Nam"]))
cc_dict = {**cc_dict_1, **cc_dict_2}
use_symbol_dict = {True: "Name", False: "Symbol"}
currencies = df["Twitter Filter"].to_list()
currencies = [x.split(", ") if "," in x else [x] for x in currencies]
count_args = load_credentials(filename="./creds.yaml", yaml_key="count_tweets_academic")

start_date = datetime.datetime.strptime("2017-06-01", "%Y-%m-%d")
end_date = datetime.datetime.strptime("2021-05-31", "%Y-%m-%d")
date_range = pd.date_range(start_date, end_date, freq="D").to_list()
month_range = pd.date_range(start_date - datetime.timedelta(1,0), end_date, freq= "M").to_list()
day_range = pd.date_range(start_date, end_date)

tweets_per_cc_per_day = int(10000000 / len(date_range) / len(currencies)) - 1
num_tweets = {}

## get tweet counts

with tqdm(total= len(currencies) * 9) as pbar:
    for currency in currencies:
        pbar.set_description(currency[0])
        queries = ["${} news crypto lang:en -is:retweet",
                   "{} news crypto lang:en -is:retweet",
                   "{} news crypto lang:en -is:retweet",
                   "${} news lang:en -is:retweet",
                   "{} news lang:en -is:retweet",
                   "{} news lang:en -is:retweet",
                   "${} lang:en -is:retweet",
                   "{} lang:en -is:retweet",
                   "{} lang:en -is:retweet"]
        use_dicts = [False, True, False, False, True, False, False, True, False]
        all_tweet_counts = pd.DataFrame()

        if os.path.isfile("./data/scraped/{}/all_counts.xlsx".format(currency[0])):
            all_tweet_counts = pd.read_excel("./data/scraped/{}/all_counts.xlsx".format(currency[0]))
            exist_queries = all_tweet_counts.columns
            new_queries = list(set(queries)- set(exist_queries))
            pbar.update(len(queries) - len(new_queries))
            queries = new_queries

        for query, use_dict in zip(queries, use_dicts):
            for sub_currency in currency:
                if (use_dict and sub_currency != cc_dict[sub_currency]) or not use_dict:
                    tweet_counts = []
                    for i in range(len(month_range) -1):
                        if use_dict:
                            count_rule = gen_request_parameters(query.format(cc_dict[sub_currency]), granularity="day", start_time=(month_range[i] + datetime.timedelta(1, 0)).strftime("%Y-%m-%d"), end_time=(month_range[i + 1] + datetime.timedelta(1, 0)).strftime("%Y-%m-%d"))
                        else:
                            count_rule = gen_request_parameters(query.format(sub_currency), granularity= "day", start_time=(month_range[i] + datetime.timedelta(1,0)).strftime("%Y-%m-%d"), end_time=(month_range[i+1] + datetime.timedelta(1,0)).strftime("%Y-%m-%d"))
                        counts = collect_results(count_rule, result_stream_args=count_args)
                        time.sleep(3.5)
                        if len(counts) > 0:
                            counts = counts[0]["data"]
                            tweet_counts.extend(counts)

                    tweet_counts = pd.DataFrame.from_records(tweet_counts)
                    tweet_counts = tweet_counts.rename(columns={'tweet_count': query + " + " + use_symbol_dict[use_dict]})
                    tweet_counts = tweet_counts.drop("end", axis=1)
                    tweet_counts = tweet_counts.set_index("start")
                    if all_tweet_counts.shape[0] == 0:
                        all_tweet_counts = tweet_counts
                    else:
                        if query + " + " + use_symbol_dict[use_dict] in all_tweet_counts.columns:
                            all_tweet_counts[query + " + " + use_symbol_dict[use_dict]] = all_tweet_counts[query + " + " + use_symbol_dict[use_dict]] + tweet_counts[query + " + " + use_symbol_dict[use_dict]]
                        else:
                            all_tweet_counts = pd.merge(all_tweet_counts, tweet_counts, how="outer", left_index= True, right_index=True)
                elif use_dict and sub_currency == cc_dict[sub_currency]:
                    if all_tweet_counts.shape[0] == 0:
                        all_tweet_counts = pd.DataFrame(columns= [query + " + "  + str(use_dict)], index = day_range, data= [np.nan] * len(day_range))
                    elif query + " + " + use_symbol_dict[use_dict] not in all_tweet_counts.columns:
                        all_tweet_counts[query + " + "  + str(use_dict)] = [np.nan] * len(day_range)

            pbar.update(1)

        if not os.path.exists("./data/scraped/{}".format(currency[0])):
            os.mkdir("./data/scraped/{}".format(currency[0]))
        all_tweet_counts.to_excel("./data/scraped/{}/all_counts.xlsx".format(currency[0]))