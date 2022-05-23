import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt

num_cc = pd.DataFrame({"year": [2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022],
                       "number of currencies": [66, 506, 562, 644, 1335, 1658, 2817, 3659, 6153, 10163]})

mkc_cc = pd.read_excel("./data/marketCap_cc.xlsx")
mkc_cc.columns = ["date", "year", "market capitalisation"]
mkc_cc["date"] = mkc_cc["date"].apply(lambda x: pd.to_datetime(x))

sns.barplot(data= num_cc, x= "year", y="number of currencies", hue=["X"]*num_cc.shape[0])
plt.title("Number of different currencies per year")
plt.legend("",frameon=False)
plt.tight_layout()
plt.savefig("./plots/numCurrPerYear.png")
plt.show()

sns.lineplot(data= mkc_cc, x= "date", y="market capitalisation")
plt.title("cumulative Marketcapitlaisation of CCs per month")
plt.tight_layout()
plt.savefig("./plots/cumMarketCapPerMonth.png")
plt.show()