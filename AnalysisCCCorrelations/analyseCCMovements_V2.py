import os
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import seaborn as sns

# loop over different years to see how the popularity between currencies has changed over years (visualization = bar chart)
for year in [2016, 2017, 2018]:
    path_old = "../CCScraper/data/cc_survival_{}_top50.xlsx".format(year)
    path_new = "../CCScraper/data/cc_survival_2021_top500_v2.xlsx"

    df_old = pd.read_excel(path_old)

    xls = pd.ExcelFile(path_new)
    df_new = pd.read_excel(xls, 'Sheet1')
    df_new = df_new.append(pd.read_excel(xls, 'Sheet2'))
    df_new = df_new.append(pd.read_excel(xls, 'Sheet3'))
    df_new = df_new.append(pd.read_excel(xls, 'Sheet4'))
    df_new = df_new.append(pd.read_excel(xls, 'Sheet5'))
    df_new.columns = ["index", "Name", "Symbol", "PriceUSD"]
    df_new = df_new.set_index("index")

    df_old["Rank"] = np.repeat(np.arange(0, int(df_old.shape[0]/10), 1), 10)
    df_old = df_old.drop("#", axis= 1)
    df_old = df_old.iloc[:30]

    df_new["Rank"] = np.repeat(np.arange(0, int(df_new.shape[0]/10), 1), 10)
    df_new_name = list(map(lambda x: [y.lower() for y in x], df_new.groupby("Rank")["Name"].apply(list)))
    df_new_symbole = list(map(lambda x: [str(y).lower() for y in x], df_new.groupby("Rank")["Symbol"].apply(list)))

    def check_new_position(row):
        row["newRank"] = np.nan
        row["newPosition"] = np.nan
        for i, (part, symbol) in enumerate(zip(df_new_name, df_new_symbole)):
            if row["Name"].lower() in part or row["Symbol"].lower() in symbol:
                row["newRank"] = i
                if row["Name"].lower() in part:
                    row["newPosition"] = part.index(row["Name"].lower()) + i*10
                else:
                    row["newPosition"] = symbol.index(row["Symbol"].lower()) + i*10
            else:
                continue
        return row

    df_old = df_old.apply(check_new_position, axis= 1)
    df_old["newRank"] = df_old["newRank"].fillna(-1)
    df_old["newPosition"] = df_old["newPosition"].fillna(500)

    new_position_avg = df_old["newPosition"].mean()
    print(new_position_avg)

    df_old["Position"] = list(range(df_old.shape[0]))

    ax = sns.histplot(data=df_old.loc[:, ["Position", "newPosition"]].melt(), x="value", hue="variable", bins= 20)
    sns.move_legend(ax, "upper right")
    plt.title("CC Market Cap Movement {} - 2021".format(year))
    plt.xlabel("Position by Market Cap")
    plt.vlines(14.5, colors="blue", label="Average Position", ymin=0, ymax=32)
    plt.annotate(text='avg:\n24.50', xy=(14.5+1, 30.5), color="blue")
    plt.vlines(new_position_avg, colors= "orange", label="Average newPosition", ymin=0, ymax= 32)
    plt.annotate(text='avg:\n{0:.2f}'.format(new_position_avg), xy=(new_position_avg+1, 30.5), color="orange")
    plt.tight_layout()
    plt.savefig("./plots/CcMarketCapMovement_{}-2021_V2.png".format(year))
    plt.show()
    plt.close()

