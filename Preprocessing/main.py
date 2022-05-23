import pandas as pd
import swifter
import torch.cuda

from tqdm import tqdm
tqdm.pandas()

import pyarrow as pa
import pyarrow.parquet as pq

from utlities import strip_tweets

from techniques import *

from transformers import BertTokenizer, BertModel, BertForSequenceClassification
from transformers import pipeline

from nltk import FreqDist

from torch.utils.data import Dataset, DataLoader

finbert = BertModel.from_pretrained('yiyanghkust/finbert-tone')
finbert_class = BertForSequenceClassification.from_pretrained('yiyanghkust/finbert-tone',num_labels=3)
tokenizer = BertTokenizer.from_pretrained('yiyanghkust/finbert-tone')
pipe_bertEmbed = pipeline("feature-extraction", model=finbert, tokenizer=tokenizer, device= -1)
pipe_bertSent = pipeline("sentiment-analysis", model=finbert_class, tokenizer=tokenizer, device= -1)

class CustomTextDataset(Dataset):
    def __init__(self, text):
        self.text = text
    def __len__(self):
            return len(self.text)
    def __getitem__(self, idx):
            return self.text[idx]

def apply_batchwise(model, dataset, batch_size= 1000, output_element= None):
    out = []
    temp = DataLoader(CustomTextDataset(dataset), batch_size= batch_size, shuffle=False)
    for batch in tqdm(temp):
        if output_element:
            out.extend(model(batch)[output_element])
        else:
            out.extend(model(batch))
    return out

# news
# {'btc': [13190, 13190], 'eth': [8349, 8349], 'xrp': [3406, 3406], 'xem': [155, 155], 'etc': [196, 196], 'ltc': [3344, 3344], 'dash': [184, 184], 'xmr': [484, 484], 'strat': [213, 213], 'xlm': [1486, 1486]}

input_path = "../FormatUnifier/Twitter.parquet"
output_path = "./data/Content_1000.parquet"
filtered_output_path = "./data/Content_filtered.parquet"

datatype = "Twitter"

BERTSentimentConversionDict = {"POSITIVE": 1, "NEGATIVE": -1, "NEUTRAL": 0}

# only look at top 10 CCs
ccs = ['btc', 'eth', 'xrp', 'xem', 'etc', 'ltc', 'dash', 'xmr', 'strat', 'xlm']

counts = {}

filtering_dict = {}
filtering_dict["all"] = 0
filtering_dict["giveaway"] = 0
filtering_dict["pump"] = 0
filtering_dict["hashtag"] = 0
filtering_dict["cashtag"] = 0
filtering_dict["minContent"] = 0
filtering_dict["filtered"] = 0


for cc in tqdm(ccs):
    print(cc)
    dataset = pq.ParquetDataset(input_path, validate_schema=False, filters=[('cc', '=', cc.upper())])
    dataset = dataset.read().to_pandas()
    full_shape = dataset.shape[0]

    #apply filtering rules
    if datatype == "Twitter":
        filtering_dict["all"] += dataset.shape[0]
        filter = pd.DataFrame()

        filter_words = ["give away", "giving away", "giveaway"]
        temp_filter = pd.DataFrame()
        for filter_word in filter_words:
            # dataset = dataset.loc[~dataset["content"].apply(lambda x: filter_word in x.lower())]
            temp = dataset["content"].apply(lambda x: filter_word in x.lower())
            temp_filter = pd.concat((temp_filter, temp), ignore_index=True, axis=1)
        filter = pd.concat((filter, temp_filter.sum(axis=1) > 0), ignore_index=True, axis=1)
        filtering_dict["giveaway"] += sum(temp_filter.sum(axis=1) > 0)

        filter_words = ["pump", "register", "join", "follow"]
        temp_filter = pd.DataFrame()
        for filter_word in filter_words:
            # dataset = dataset.loc[~dataset["content"].apply(lambda x: filter_word in x.lower())]
            temp = dataset["content"].apply(lambda x: filter_word in x.lower())
            temp_filter = pd.concat((temp_filter, temp), ignore_index=True, axis=1)
        filter = pd.concat((filter, temp_filter.sum(axis=1) > 0), ignore_index=True, axis=1)
        filtering_dict["pump"] += sum(temp_filter.sum(axis=1) > 0)

        # dataset = dataset.loc[dataset["content"].apply(lambda x: x.count("#") < 14)]
        filter = pd.concat((filter, dataset["content"].apply(lambda x: x.count("#") >= 14)), ignore_index=True, axis=1)
        filtering_dict["hashtag"] += sum(dataset["content"].apply(lambda x: x.count("#") >= 14))
        # dataset = dataset.loc[dataset["content"].apply(lambda x: x.count("$") < 14)]
        filter = pd.concat((filter, dataset["content"].apply(lambda x: x.count("$") >= 14)), ignore_index=True, axis=1)
        filtering_dict["cashtag"] += sum(dataset["content"].apply(lambda x: x.count("$") >= 14))

        filter = pd.concat((filter, dataset["content"].apply(lambda x: len(strip_tweets(x)) < 20)), ignore_index=True, axis=1)
        filtering_dict["minContent"] += sum(dataset["content"].apply(lambda x: len(strip_tweets(x)) < 20))

        filter = filter.sum(axis=1) < 2

        dataset = dataset.loc[filter]

        filtering_dict["filtered"] += dataset.shape[0]

        # track how much data is lost due to filtering
        counts[cc] = [full_shape, dataset.shape[0]]
    else:
        counts[cc] = [full_shape, full_shape]

    # Write direct to parquet file
    table = pa.Table.from_pandas(dataset)
    pq.write_to_dataset(table, root_path=filtered_output_path, partition_cols=['date', 'source', 'cc'])

    #start preprocessing
    dataset.rename(columns = {'content':'content_raw'}, inplace = True)

    # for testing
    #dataset = dataset.iloc[:300, :]

    dataset["content_bertEmbed"] = apply_batchwise(pipe_bertEmbed, dataset["content_raw"].to_list())

    # clear up text
    dataset["content_processed"] = dataset.progress_apply(lambda x: preprocessor(x["content_raw"],x["cc"]),axis=1)
    dataset["content_raw"] = dataset.progress_apply(lambda x: preprocessor_lite(x["content_raw"], x["cc"]), axis=1)

    # remove 50 most common words
    most_common_words = FreqDist(np.concatenate(dataset["content_processed"].apply(lambda x: np.array(x.split(" "))).values)).most_common(50)


    # create w2v embeds
    dataset["content_w2v"] = dataset["content_processed"].progress_apply(lambda x: generateW2V(x))
    dataset["content_w2vSum"] = dataset["content_w2v"].progress_apply(lambda x: x.mean(axis=0).tolist())
    dataset["content_w2v"] = dataset["content_w2v"].progress_apply(lambda x: x.tolist())
    dataset = dataset.drop("content_w2v", axis= 1)
    dataset["content_w2vToken"] = dataset["content_processed"].progress_apply(lambda x: generateW2VToken(x))

    # create FinBERT embeds
    dataset["content_bertEmbed"] = apply_batchwise(pipe_bertEmbed, dataset["content_raw"].to_list())
    dataset["content_bertEmbed"] = dataset["content_bertEmbed"].progress_apply(lambda x: x[0][0])
    dataset["content_bertToken"] = tokenizer(dataset["content_raw"].to_list())["input_ids"]

    # create Loughran McDonald sentiment scores
    dataset["sentiment_LM"] = dataset["content_processed"].progress_apply(lambda x: generateLMSentimentScore(x))

    # create Vader sentiment scores
    dataset["sentiment_Vader"] = dataset["content_processed"].progress_apply(lambda x: generateVaderSentimentScore(x))

    # create bert sentiment scores
    dataset["sentiment_bert"] = pipe_bertSent(dataset["content_raw"].to_list())
    dataset["sentiment_bert"] = dataset["sentiment_bert"].progress_apply(lambda x: BERTSentimentConversionDict[x["label"]] * x["score"])

    # Write direct to parquet file
    table = pa.Table.from_pandas(dataset)
    pq.write_to_dataset(table, root_path=output_path, partition_cols=['date', 'source', 'cc'])