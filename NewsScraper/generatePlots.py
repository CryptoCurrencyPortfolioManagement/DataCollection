import pandas as pd
from tqdm import tqdm
tqdm.pandas()
from matplotlib import pyplot as plt
from matplotlib.patches import Rectangle
import seaborn as sns
from scipy.interpolate import make_interp_spline

def check_cc_mentioned(text: str, cc_dict: dict):
    text = text.lower()
    ccs_in_text = list()
    for key in cc_dict.keys():
        for alt in cc_dict[key]:
            if alt in text:
                ccs_in_text.append(key)
    return ccs_in_text


selected_ccs = ['btc', 'eth', 'xrp', 'xem', 'etc', 'ltc', 'dash', 'xmr', 'strat', 'xlm']
news_types = ["bitcoin", "ripple", "ethereum", "litecoin", "altcoin"]

ccs = pd.read_excel("../CCScraper/data/cc_master.xlsx", index_col= 0)
ccs["all"] = ccs.apply(lambda x: list(set([x for x in (str(x["Name"]).lower() + ", " + str(x["Symbol"]).lower() + ", " + str(x["Alt Nam"]).lower() + ", " + str(x["Alt Symbol"]).lower()).split(", ") if not x == "nan"])), axis= 1).to_list()
cc_dict = dict(zip(ccs["Symbol"], ccs["all"]))

start_date = pd.to_datetime("2017-06-01").date()
end_date = pd.to_datetime("2021-05-31").date()

df = pd.DataFrame()
unique_articles = 0

for news_type in news_types:
    temp = pd.read_csv("./data/articles_{}.csv".format(news_type))
    temp["cc"] = temp.progress_apply(lambda x: check_cc_mentioned(x["header"] + x["text"], cc_dict), axis=1)
    unique_articles += temp.shape[0]
    temp = temp.explode("cc")
    df = df.append(temp)

df = df.reset_index(drop= True)
df = df.loc[df["cc"].str.lower().apply(lambda x: x in selected_ccs)]

df["date"] = df["date"].progress_apply(lambda x: pd.to_datetime(x).date())
df = df.loc[(df["date"] < end_date) & (df["date"] > start_date)]

df["cc"] = df["cc"].str.lower()
df["cc"] = pd.Categorical(df["cc"], categories=selected_ccs, ordered=True)

# create plots displaying the text datas characteristics before pre-processing
sns.histplot(data= df, x= "cc")
plt.title("Number of articles per CC")
plt.annotate("Unique articles:\n {}".format(unique_articles), (7, 0.90*df["cc"].value_counts().max()))
plt.tight_layout()
plt.savefig("./plots/NumNewsPerCc_beforePreProcess.png")
plt.show()

df.loc[:, "header length"] = df["header"].apply(lambda x: len(str(x).split())).astype(float)
sns.violinplot(data= df.drop_duplicates(subset= "header"), x="header length")
plt.title("Number of words in article header")
plt.tight_layout()
plt.savefig("./plots/LenNews_beforePreProcess.png")
plt.show()

df_by_date = df.drop_duplicates(subset= "header").groupby("date")["cc"].count().reset_index(drop= False)
df_by_date = df_by_date.rename(columns= {"cc": "Number of articles"})
df_by_date["Moving avg (30 days)"] = df_by_date["Number of articles"].rolling(30, min_periods=1).mean()
df_by_date = df_by_date.melt(id_vars="date")
sns.lineplot(data=df_by_date, x= "date", y="value", hue="variable")
plt.ylabel("Number of articles")
plt.title("Number of articles per day")
plt.tight_layout()
plt.savefig("./plots/NumNewsPerDay_beforePreProcess.png")
plt.show()

df.loc[:,["date","cc","header length", "header"]].to_pickle("./data/News_metricsDf.pkl")