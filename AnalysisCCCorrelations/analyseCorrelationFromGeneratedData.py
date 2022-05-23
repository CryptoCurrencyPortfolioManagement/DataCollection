import os
import glob

import pandas as pd
import numpy as np

import re

import seaborn as sns
from matplotlib import pyplot as plt

# analyse the correlation of different cc portfolios of varying size during the evaluation period
year= 2017

folders= glob.glob("./data/lookForward/{}_top[0-9]*/".format(year))

corr_df = pd.DataFrame()

for folder in folders:
    numAssets = int(re.search("(?<=top)\d+", folder).group(0))

    pearCorr_df = pd.read_csv(folder + "cc_pearCorr_returns.csv", index_col= 0)
    spearCorr_df = pd.read_csv(folder + "cc_spearCorr_returns.csv", index_col= 0)

    avg_pear_cors = pearCorr_df.values.flatten()[~np.isnan(pearCorr_df.values.flatten())].mean()
    avg_spear_cors = spearCorr_df.values.flatten()[~np.isnan(spearCorr_df.values.flatten())].mean()

    min_pear_cors = pearCorr_df.values.flatten()[~np.isnan(pearCorr_df.values.flatten())].min()
    min_spear_cors = spearCorr_df.values.flatten()[~np.isnan(spearCorr_df.values.flatten())].min()

    corr_df = corr_df.append(pd.Series({"numAssets": numAssets, "PearCorr": avg_pear_cors, "SpearCorr": avg_spear_cors}), ignore_index= True)

corr_df = corr_df.sort_values("numAssets")
sns.lineplot(data= corr_df, x= "numAssets", y= "PearCorr")
plt.vlines(x= 10, colors= "red", ymin= corr_df["PearCorr"].min(), ymax= corr_df["PearCorr"].max())
plt.title("Pearson Correlation vs. Number of Assets ({} - 2021)".format(year))
plt.tight_layout()
plt.savefig("./plots/pearCorr_vs_numAssets_{}-{}.png".format(year, 2021))
plt.show()

sns.lineplot(data= corr_df, x= "numAssets", y= "SpearCorr")
plt.vlines(x= 10, colors= "red", ymin= corr_df["SpearCorr"].min(), ymax= corr_df["SpearCorr"].max())
plt.title("Spearman Correlation vs. Number of Assets ({} - 2021)".format(year))
plt.tight_layout()
plt.savefig("./plots/spearCorr_vs_numAssets_{}-{}.png".format(year, 2021))
plt.show()