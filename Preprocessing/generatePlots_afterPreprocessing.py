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

def check_cc_mentioned(text: str, cc_dict: dict):
    text = text.lower()
    ccs_in_text = list()
    for key in cc_dict.keys():
        for alt in cc_dict[key]:
            if alt in text:
                ccs_in_text.append(key)
    return ccs_in_text

# use preprocessed content to create various plots
input_path = "./data/Content.parquet"
unique_articles = 0

ccs = ['btc', 'eth', 'xrp', 'xem', 'etc', 'ltc', 'dash', 'xmr', 'strat', 'xlm']

start_date = pd.to_datetime("2017-06-01").date()
end_date = pd.to_datetime("2021-05-31").date()

df = pd.DataFrame()

for source in ["CoinTelegraph", "Twitter"]:
    for cc in tqdm(ccs):
        print(cc)
        dataset = pq.ParquetDataset(input_path, validate_schema=False, filters=[('cc', '=', cc), ('source', '=', source)])
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

    df.loc[:, "text length old"] = df["content_raw"].apply(lambda x: len(str(x).split()))
    df.loc[:, "text length new"] = df["content_processed"].apply(lambda x: len(str(x).split()))
    temp1 = df.loc[:, ["text length old", "content_raw"]]
    temp1 = temp1.rename(columns={"text length old": "text length", "content_raw": "text"})
    temp1["Process step"] = "before Preprocessing"
    temp2 = df.loc[:, ["text length new", "content_processed"]]
    temp2 = temp2.rename(columns={"text length new": "text length", "content_processed": "text"})
    temp2["Process step"] = "after Preprocessing"
    df_plot = pd.concat((temp1, temp2), axis= 0, ignore_index= True)
    df_plot = df_plot.drop_duplicates(subset= ["text", "Process step"])
    df_plot["all"] = ""
    sns.violinplot(data= df_plot, x="text length", y="all", hue="Process step", hue_order=["before Preprocessing", "after Preprocessing"], split=True)
    plt.title("Number of words in texts")
    plt.tight_layout()
    if source == "Twitter":
        plt.savefig("./plots/Twitter_LenTexts_afterPreprocessing.png".format(source))
    else:
        plt.savefig("./plots/News_LenTexts_afterPreprocessing.png".format(source))
    plt.show()