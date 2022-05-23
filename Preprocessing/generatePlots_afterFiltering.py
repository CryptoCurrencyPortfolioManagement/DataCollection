import csv
from datetime import datetime
import pandas as pd
from tqdm import tqdm
tqdm.pandas()
from matplotlib import pyplot as plt
from matplotlib.patches import Rectangle
import seaborn as sns
from scipy.interpolate import make_interp_spline
import pyarrow as pa
import pyarrow.parquet as pq
from empyrical import cum_returns
import statsmodels.formula.api as smf

def check_cc_mentioned(text: str, cc_dict: dict):
    text = text.lower()
    ccs_in_text = list()
    for key in cc_dict.keys():
        for alt in cc_dict[key]:
            if alt in text:
                ccs_in_text.append(key)
    return ccs_in_text

# use filtered content to create various plots
input_path = "./data/Content_filtered.parquet"
unique_articles = 0

ccs = ['btc', 'eth', 'xrp', 'xem', 'etc', 'ltc', 'dash', 'xmr', 'strat', 'xlm']

start_date = pd.to_datetime("2017-06-01").date()
end_date = pd.to_datetime("2021-05-31").date()

df = pd.DataFrame()

for cc in tqdm(ccs):
    print(cc)
    dataset = pq.ParquetDataset(input_path, validate_schema=False, filters=[('cc', '=', cc.upper())])
    dataset = dataset.read().to_pandas()
    df = pd.concat((df, dataset), ignore_index=True, axis=0)

df = df.reset_index(drop= True)
df = df.loc[df["cc"].str.lower().apply(lambda x: x in ccs)]

df["date"] = df["date"].astype(str)
df["date"] = df["date"].progress_apply(lambda x: pd.to_datetime(x).date())
df = df.loc[(df["date"] < end_date) & (df["date"] > start_date)]

df = df.loc[df["cc"].str.lower().apply(lambda x: x in ccs)]
df["cc"] = df["cc"].str.lower()
df["cc"] = pd.Categorical(df["cc"], categories=ccs, ordered=True)

df_old = pd.read_pickle("../TwitterScraper/data/Twitter_metricsDf.pkl")

df["Process step"] = "after Filtering"
df_old["Process step"] = "before Filtering"

df = df.reset_index(drop= True)
df_plot = pd.concat((df, df_old), axis= 0, ignore_index= True)
sns.histplot(data= df_plot, x= "cc", hue="Process step", hue_order=["after Filtering","before Filtering"])
plt.title("Number of tweets per CC")
plt.tight_layout()
plt.savefig("./plots/NumNewsPerCc_afterFiltering.png")
plt.show()

df_plot = pd.concat((df_plot.loc[df_plot["Process step"] == "before Filtering", "cc"].value_counts(), df_plot.loc[df_plot["Process step"] == "after Filtering", "cc"].value_counts()), axis= 1)
df_plot.columns = ["before Filtering", "after Filtering"]
df_plot["Filtered out Tweets"] = df_plot["before Filtering"] - df_plot["after Filtering"]
df_plot["Number of Querries"] = (pd.read_csv("../TwitterScraper/data/SelectedQueriesByCurrency.csv", delimiter=";").iloc[1:] == "x").sum(axis= 1).to_list()
sns.scatterplot(data= df_plot, x="Filtered out Tweets", y= "Number of Querries")
plt.title("Number of filtered out tweets vs Number of querries")
plt.tight_layout()
plt.savefig("./plots/FilteredvsNumQueries_afterFiltering.png")
plt.show()

df.loc[:, "text length"] = df["content"].apply(lambda x: len(str(x).split()))
df = df.rename(columns= {"content": "text"})
df_plot = pd.concat((df, df_old), axis= 0, ignore_index= True)
df_plot = df_plot.drop_duplicates(subset= ["text", "Process step"])
df_plot["all"] = ""
sns.violinplot(data= df_plot, x="text length", y="all", hue="Process step", hue_order=["after Filtering","before Filtering"], split=True)
plt.title("Number of words in tweet")
plt.tight_layout()
plt.savefig("./plots/LenTweets_afterFiltering.png")
plt.show()

df_plot = pd.concat((df, df_old), axis= 0, ignore_index= True)
df_by_date = df_plot.groupby(["date", "Process step"])["cc"].count().reset_index(drop= False)
df_by_date = df_by_date.rename(columns= {"cc": "Number of articles"})
df_by_date.loc[df_by_date["Process step"] == "before Filtering", "Moving avg (30 days)"] = df_by_date.loc[df_by_date["Process step"] == "before Filtering", "Number of articles"].rolling(30, min_periods=1).mean()
df_by_date.loc[df_by_date["Process step"] == "after Filtering", "Moving avg (30 days)"] = df_by_date.loc[df_by_date["Process step"] == "after Filtering", "Number of articles"].rolling(30, min_periods=1).mean()
sns.lineplot(data=df_by_date, x= "date", y="Moving avg (30 days)", hue="Process step", hue_order=["after Filtering","before Filtering"])
plt.ylabel("Number of articles")
plt.title("Number of articles per day")
plt.tight_layout()
plt.savefig("./plots/NumTweetsPerDay_afterFiltering.png")
plt.show()

diff_by_date = df_by_date.groupby("date").apply(lambda group: group.loc[group["Process step"]=="before Filtering","Number of articles"].mean() - group.loc[group["Process step"]=="after Filtering","Number of articles"].mean())
diff_by_date = diff_by_date.reset_index(drop=False)
diff_by_date.columns = ["date", "Diff in number of articles"]
diff_by_date["Diff in number of articles avg 7"] = diff_by_date["Diff in number of articles"].rolling(7, min_periods=1).mean()
diff_by_date["Diff in number of articles avg 30"] = diff_by_date["Diff in number of articles"].rolling(30, min_periods=1).mean()

df_plot = pd.concat((df, df_old), axis= 0, ignore_index= True)
df.value_counts(["Process step", "cc"])
price_df = pq.ParquetDataset("../FormatUnifier/Price.parquet", validate_schema=False, filters=[('cc', 'in', ccs)])
price_df = price_df.read(columns=["price"]).to_pandas()
price_df = price_df.pivot(index='date', columns='cc', values='price')
price_df = price_df.reset_index()
price_df["date"] = pd.to_datetime(price_df["date"]).apply(lambda x: x.date())
price_df["avg_return"] = ((price_df.iloc[:,1:].shift(-1)/price_df.iloc[:,1:])-1).mean(axis=1)
equal_weights = 1 / (price_df.iloc[0,1:-1] / price_df.iloc[0,1:-1].max())
price_df["avg_price"] = price_df.iloc[:,1:-1].values.dot(equal_weights).astype(float) / price_df.iloc[:,1:-1].values.dot(equal_weights)[0]
price_df["Moving avg (30 days)"] = price_df["avg_return"].rolling(30, min_periods=1).mean()
price_df["Moving avg (7 days)"] = price_df["avg_return"].rolling(7, min_periods=1).mean()
price_df["cumReturns"] = cum_returns(price_df["avg_return"])

plot_df = pd.merge(price_df,diff_by_date,on="date")
plot_df = plot_df.loc[:, ["date", "cumReturns", "Diff in number of articles"]].melt(id_vars="date")
sns.lineplot(data=price_df, x="date", y="cumReturns")
sns.lineplot(data=diff_by_date, x="date", y="Diff in number of articles avg 7")

#ax = price_df.plot(x="date", y="avg_price", legend=False)
#ax2 = ax.twinx()
#diff_by_date.plot(x="date", y="Diff in number of articles avg 7", ax=ax2, legend=False, color="#ff7f0e")
#ax.figure.legend()
#ax.tick_params(axis="x", rotation=50)
#plt.tight_layout()
#plt.show()

def toBeginOfWeek(x):
    return datetime.strptime(str(x.year) + "-" + str(x.isocalendar()[1]) + "-1", "%Y-%W-%w")

corr_df = price_df.merge(diff_by_date, on= "date", how= "left").dropna()
corr_df = corr_df.rename(columns= {"Diff in number of articles": "DiffNum"})
corr_df["startDate_week"] = corr_df["date"].apply(toBeginOfWeek).dt.date
corr_df = corr_df.merge(df_old.groupby("date").count()["text"].reset_index(drop= False), on="date")
corr_df["DiffPct"] = corr_df["DiffNum"] / corr_df["text"]

corr_df = corr_df.sort_values("date")
ax = corr_df.groupby("startDate_week").mean().reset_index().plot(x="startDate_week", y="avg_price", legend=True, label= "Avg price per week")
ax2 = ax.twinx()
corr_df.groupby("startDate_week").mean().reset_index().plot(x="startDate_week", y="DiffPct", ax=ax2, legend=True, color="#ff7f0e", label= "Pct difference in tweets per week")
ax.tick_params(axis="x", rotation=50)
ax.set_ylabel("normalised average price per week\n(for equally weighted CC portfolio)")
ax2.set_ylabel("Average difference in tweets per week")
plt.title("Percentage difference in articles vs. CC price per week")
ax.set_xlabel("date")
plt.tight_layout()
plt.savefig("./plots/DiffTweetsPerWeek_afterFiltering.png")
plt.show()

corr_df.loc[:, "DiffPct"] = corr_df["DiffPct"] / corr_df["DiffPct"].max()

res = smf.ols(formula="avg_price ~ DiffPct", data=corr_df.groupby("startDate_week").mean()).fit()
print("Complete")
print(res.summary())
resultFile = open("./models/complete_Price-Diff_model.csv",'w')
resultFile.write(res.summary().as_csv())
resultFile.close()

temp = corr_df.loc[(corr_df["date"] < pd.to_datetime("2018-07-01").date()) | (corr_df["date"] > pd.to_datetime("2020-01-01").date())]
res = smf.ols(formula="avg_price ~ DiffPct", data=temp.groupby("startDate_week").mean()).fit()
print("Selected Timeperiod")
print(res.summary())
resultFile = open("./models/normalPeriod_Price-Diff_model.csv",'w')
resultFile.write(res.summary().as_csv())
resultFile.close()

temp = corr_df.loc[(corr_df["date"] > pd.to_datetime("2018-07-01").date()) & (corr_df["date"] < pd.to_datetime("2020-01-01").date())]
res = smf.ols(formula="avg_price ~ DiffPct", data=temp.groupby("startDate_week").mean()).fit()
print("Selected Timeperiod")
print(res.summary())
resultFile = open("./models/abnormalPeriod_Price-Diff_model.csv",'w')
resultFile.write(res.summary().as_csv())
resultFile.close()