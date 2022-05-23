import os
import numpy as np
import pandas as pd
import plotly.graph_objects as go

# loop over different years to see how the popularity between currencies has changed over years (visualization = sankey diagram)
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
    #df_old = df_old.iloc[:10]

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

    df_old_grouped = df_old.groupby("Rank")

    def combineNodeGroup(group, max_epoch_1, max_epoch_2):
        table = []
        for i, count in group["newRank"].value_counts().iteritems():
            row = []
            row.append(int(group["Rank"].iloc[0]))
            if i == -1:
                row.append(max_epoch_2+max_epoch_1)
            else:
                row.append(int(i)+max_epoch_1)
            row.append(int(count))
            table.append(row)
        return pd.DataFrame(table, columns=["Rank", "newRank", "value"])


    df_old_grouped = df_old_grouped.apply(combineNodeGroup, max_epoch_1=len(df_old["Rank"].unique()), max_epoch_2= len(df_new["Rank"].unique()))
    df_old_grouped = df_old_grouped.reset_index(drop= True)

    plotly_config = dict()
    fig = go.Figure(data=[go.Sankey(
        arrangement = "fixed",
        node = dict(
          pad = 3,
          label = ["{} [{}:{}]".format(year, x*10, x*10+10) for x in df_old["Rank"].unique()] + ["2021 [{}:{}]".format(x*10, x*10+10) for x in df_new["Rank"].unique()] + ["Below Top500"],
          x = [0]*len(df_old["Rank"].unique()) + [1]*(len(df_new["Rank"].unique())+1),
          y = [round((x+1), 4)  for x in df_old["Rank"].unique().tolist()] + [round((x+1)*0.07, 4)  for x in df_new["Rank"].unique().tolist() + [len(df_new["Rank"].unique())]],
          color = "blue"
        ),
        link = dict(
          source = df_old_grouped["Rank"].tolist(),
          target = df_old_grouped["newRank"].tolist(),
          value = df_old_grouped["value"].tolist()
      ))])

    fig.update_layout(title_text="CC Market Capitalisation Movement {} - 2021".format(year), font_size=10)
    fig.show(config= plotly_config)
    fig.write_image("plots/CcMarketCapMovement_{}-2021.png".format(year))

    new_position_avg = df_old["newPosition"].mean()
    print(new_position_avg)

