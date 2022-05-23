import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
tqdm.pandas()
import time
from nordvpn_switcher import initialize_VPN, rotate_VPN

initialize_VPN(save=1, area_input=['random countries europe 25'])
rotate_VPN(google_check = 1)
time.sleep(5)

#df = pd.read_excel("../CCScraper/data/cc_survival_2017_top50.xlsx")
df = pd.read_excel("./data/cc_survival_2017_top50_redditLinks.xlsx")
def create_reddit_links(row):
    reddit_links = []
    reddit_links.append(row["Name"])
    reddit_links.append(row["Symbol"])
    reddit_links.append(row["Name"] + row["Symbol"])
    reddit_links.append(row["Name"] + "_" + row["Symbol"])
    if not pd.isna(row["Alt Nam"]):
        for element in row["Alt Nam"].split(", "):
            reddit_links.append(element)
    if not pd.isna(row["Alt Symbol"]):
        for element in row["Alt Symbol"].split(", "):
            reddit_links.append(element)
    if not pd.isna(row["Alt Nam"]) and not pd.isna(row["Alt Symbol"]):
        Name = row["Alt Nam"].split(", ")[0]
        Symbol = row["Alt Symbol"].split(", ")[0]
        reddit_links.append(Name + Symbol)
        reddit_links.append(Name + "_" + Symbol)
    reddit_links = [[reddit_link, reddit_link + "Markets", reddit_link + "Market", reddit_link + "Traders", reddit_link + "Trader"] for reddit_link in reddit_links]
    reddit_links = [item for sublist in reddit_links for item in sublist]
    row["Reddit links"] = reddit_links
    return row

df = df.apply(create_reddit_links, axis= 1)

i = 0

def scrape_data(row):
    global i
    num_users = []
    row["Reddit links"] = list(dict.fromkeys(row["Reddit links"]))
    for subreddit in row["Reddit links"]:
        time.sleep(1)
        response = requests.get("http://www.reddit.com/r/{}/about.json".format(subreddit), headers={"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"})
        if response.status_code == 200:
            response = response.json()
            if response["kind"] == "t5":
                num_user = int(response["data"]["subscribers"])
            else:
                num_user = np.nan
        else:
            num_user = np.nan
        num_users.append(num_user)
    row["Reddit popularity"] = num_users
    if i % 10 == 0 and i != 0:
        rotate_VPN(google_check=1)
        time.sleep(3)
    i += 1

    return row

df = df.progress_apply(scrape_data, axis= 1)

def create_reddit_links(row):
    row_filter = [pd.isna(x) for x in row["Reddit popularity"]]
    try:
        reddit_links = [x for i, x in enumerate(row["Reddit links"]) if not row_filter[i]]
    except:
        pass
    reddit_links = ["http://www.reddit.com/r/{}".format(link) for link in reddit_links]
    row["True Reddit Links"] = reddit_links
    row["Num User"] = sum([x for i, x in enumerate(row["Reddit popularity"]) if not row_filter[i]])
    return row

df = df.apply(create_reddit_links, axis= 1)

df.to_excel("./data/cc_survival_2017_top50_redditLinks.xlsx")

print(df)