import os
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from tqdm import tqdm
import random
import datetime
from searchtweets import ResultStream, gen_request_parameters, load_credentials, collect_results
import time
import pickle


df = pd.read_excel("./data/cc_survival_2017_top50_twitter.xlsx")
cc_dict_1 = dict(zip(df["Symbol"], df["Name"]))
cc_dict_2 = dict(zip(df.loc[~df["Alt Symbol"].isnull()]["Alt Symbol"], df.loc[~df["Alt Symbol"].isnull()]["Alt Nam"]))
cc_dict = {**cc_dict_1, **cc_dict_2}
currencies = df["Twitter Filter"].to_list()
currencies = [x.split(", ") if "," in x else [x] for x in currencies]
search_args = load_credentials(filename="./creds.yaml", yaml_key="search_tweets_academic")

start_date = datetime.datetime.strptime("2017-06-01", "%Y-%m-%d")
end_date = datetime.datetime.strptime("2021-05-31", "%Y-%m-%d")
date_range = pd.date_range(start_date, end_date, freq="D").to_list()
month_range = pd.date_range(start_date - datetime.timedelta(1,0), end_date, freq= "M").to_list()

tweets_per_cc = int((10000000  - 1000000) / len(currencies))

counts = pd.read_excel("./data/scraped/counts.xlsx", index_col=0)

not_processed = list()

def get_queries_from_counts(row):
    global not_processed
    row = row.loc[row != 0]
    row = row.sort_values()
    row_sum = list(np.cumsum(row))
    selector = [y for y in row_sum if y < tweets_per_cc]
    if len(selector) > 0:
        return pd.Series({"queries": list(row.index[: len(selector)]), "num_tweets": selector[-1], "acceptable": True})
    else:
        print("exceeded num tweets for {}:   {} / {}".format(row.name, row.iloc[0], tweets_per_cc))
        not_processed.append({"Symbol": row.name, "min_count": row.iloc[0]})
        return pd.Series({"queries": list(), "num_tweets": row.iloc[0], "acceptable": False})

queries = counts.apply(get_queries_from_counts, axis= 1)
queries["num_tweets"].plot.bar()
plt.title("Collected tweets per currency")
plt.savefig("./plots/tweetDistribution.png")
plt.show()

if len(not_processed) > 0:
    not_processed = pd.DataFrame.from_records(not_processed)
    not_processed.to_excel("./data/scraped/notprocessed.xlsx")

with tqdm(total= sum(queries["queries"].apply(lambda x: len(x)))) as pbar:
    for currency in currencies:
        pbar.set_description(currency[0])
        all_tweets = {}

        temp_queries = queries.loc[currency[0], "queries"]

        if os.path.isfile("./data/scraped/{}/tweets.pkl".format(currency[0])):
            with open("./data/scraped/{}/tweets.pkl".format(currency[0]), 'rb') as handle:
                all_tweets = pickle.load(handle)
            exist_queries = all_tweets.keys()
            new_queries = list(set(temp_queries)- set(exist_queries))
            pbar.update(len(temp_queries) - len(new_queries))
            temp_queries = new_queries

        for full_query in temp_queries:
            if " + " in full_query:
                use_dict = True if full_query.split(" + ")[-1] == "Name" else False
                query = " + ".join(full_query.split(" + ")[:-1])
            else:
                use_dict = False
                query = full_query
            tweets = []
            #iterate over the different names of a currency including 3 letter abreviation
            for sub_currency in currency:
                if (use_dict and sub_currency != cc_dict[sub_currency]) or not use_dict:
                    if use_dict:
                        search_rule = gen_request_parameters(query.format(cc_dict[sub_currency]),
                                                             start_time= start_date.strftime("%Y-%m-%dT%H:%M"),
                                                             end_time= end_date.strftime("%Y-%m-%dT%H:%M"),
                                                             tweet_fields="id,created_at,author_id,public_metrics,entities,geo,text",
                                                             results_per_call= 500,
                                                             granularity= None)
                    else:
                        search_rule = gen_request_parameters(query.format(sub_currency),
                                                             start_time=start_date.strftime("%Y-%m-%dT%H:%M"),
                                                             end_time=end_date.strftime("%Y-%m-%dT%H:%M"),
                                                             tweet_fields="id,created_at,author_id,public_metrics,entities,geo,text",
                                                             results_per_call= 500,
                                                             granularity= None)

                    response = collect_results(search_rule, max_tweets= tweets_per_cc, result_stream_args=search_args)
                    time.sleep(3.5)

                    if len(response) > 0:
                        for sub_response in response:
                            tweets.extend(sub_response["data"])

            tweets = pd.DataFrame.from_records(tweets)
            if full_query not in all_tweets.keys():
                all_tweets[full_query] = tweets
            else:
                all_tweets[full_query] = all_tweets[full_query].append(tweets)

            pbar.update(1)

        if not os.path.exists("./data/scraped/{}".format(currency[0])):
            os.mkdir("./data/scraped/{}".format(currency[0]))
        with open("./data/scraped/{}/tweets.pkl".format(currency[0]), 'wb') as handle:
            pickle.dump(all_tweets, handle, protocol=pickle.HIGHEST_PROTOCOL)


