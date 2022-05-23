import os
import pandas as pd
from tqdm import tqdm
import time
from matplotlib import pyplot as plt

currencies = [x for x in os.listdir("./data/scraped") if not "." in x]

count_currencies = {}

for currency in tqdm(currencies):
    #time.sleep(0.5)
    #print(currency)
    temp = pd.read_excel("./data/scraped/" + currency + "/all_counts.xlsx")
    temp = temp[[x for x in temp.columns if ("{}" in x) or x == "start"]]
    temp = temp.set_index("start")
    temp_sum = temp.sum(axis= 0)
    count_currencies[currency] = temp_sum.tolist()
    columns = temp_sum.index

count_currencies = pd.DataFrame.from_dict(count_currencies, orient='index', columns= columns)

# do plotting
for i, column in enumerate(count_currencies.columns):
    count_currencies[column].plot.bar()
    plt.title(column)
    plt.savefig("./plots/query{}.png".format(i))
    plt.show()

for i, row in enumerate(count_currencies.index):
    count_currencies.loc[row].plot.bar()
    plt.title(row)
    plt.savefig("./plots/query{}.png".format(i))
    plt.show()


count_currencies.to_excel("./data/scraped/counts.xlsx")

