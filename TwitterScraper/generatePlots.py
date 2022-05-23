import pandas as pd
from tqdm import tqdm
tqdm.pandas()
from matplotlib import pyplot as plt
from matplotlib.patches import Rectangle
import seaborn as sns

def check_cc_mentioned(text: str, cc_dict: dict):
    text = text.lower()
    ccs_in_text = list()
    for key in cc_dict.keys():
        for alt in cc_dict[key]:
            if alt in text:
                ccs_in_text.append(key)
    return ccs_in_text

# create some visualization of the raw content
selected_ccs = ['btc', 'eth', 'xrp', 'xem', 'etc', 'ltc', 'dash', 'xmr', 'strat', 'xlm']
news_types = ["bitcoin", "ripple", "ethereum", "litecoin", "altcoin"]

ccs = pd.read_excel("../CCScraper/data/cc_master.xlsx", index_col= 0)
ccs["all"] = ccs.apply(lambda x: list(set([x for x in (str(x["Name"]).lower() + ", " + str(x["Symbol"]).lower() + ", " + str(x["Alt Nam"]).lower() + ", " + str(x["Alt Symbol"]).lower()).split(", ") if not x == "nan"])), axis= 1).to_list()
cc_dict = dict(zip(ccs["Symbol"], ccs["all"]))

start_date = pd.to_datetime("2017-06-01").date()
end_date = pd.to_datetime("2021-05-31").date()

df = pd.DataFrame()
unique_articles = 0

for cc in tqdm(selected_ccs):
    temp = pd.read_pickle("../TwitterScraper/data/scraped/{}/tweets.pkl".format(cc))
    for key in temp.keys():
        temp[key]["cc"] = cc
        temp[key] = temp[key].drop(list(set(temp[key].columns)-{"created_at", "text", "cc"}), axis= 1)
        df = df.append(temp[key])

df["date"] = df["created_at"].progress_apply(lambda x: pd.to_datetime(x).date())
df = df.loc[(df["date"] < end_date) & (df["date"] > start_date)]

df = df.reset_index(drop= True)
df = df.loc[df["cc"].str.lower().apply(lambda x: x in selected_ccs)]

df["cc"] = df["cc"].str.lower()
df["cc"] = pd.Categorical(df["cc"], categories=selected_ccs, ordered=True)

sns.histplot(data= df, x= "cc")
plt.title("Number of tweets per CC")
plt.tight_layout()
plt.savefig("./plots/NumTweetsPerCc_beforePreProcess.png")
plt.show()

df.loc[:, "text length"] = df["text"].apply(lambda x: len(str(x).split()))
sns.violinplot(data= df.drop_duplicates(subset= "text"), x="text length")
plt.title("Number of words in tweet")
plt.tight_layout()
plt.savefig("./plots/LenTweets_beforePreProcess.png")
plt.show()

df_by_date = df.groupby("date")["cc"].count().reset_index(drop= False)
df_by_date = df_by_date.rename(columns= {"cc": "Number of tweets"})
df_by_date["Moving avg (30 days)"] = df_by_date["Number of tweets"].rolling(30, min_periods=1).mean()
df_by_date = df_by_date.melt(id_vars="date")
sns.lineplot(data=df_by_date, x= "date", y="value", hue="variable")
plt.ylabel("Number of tweets")
plt.title("Number of tweets per day")
plt.tight_layout()
plt.savefig("./plots/NumTweetsPerDay_beforePreProcess.png")
plt.show()

df.loc[:,["date","cc","text length","text"]].to_pickle("./data/Twitter_metricsDf.pkl")