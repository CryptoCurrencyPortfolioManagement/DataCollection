import os
import numpy as np
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
from datetime import datetime
from tqdm import tqdm
import re

#setup nordvpn
from nordvpn_switcher import initialize_VPN, rotate_VPN
initialize_VPN(save=1, area_input=['random countries europe 25'])
rotate_VPN(google_check = 1)


# setup of selenium driver
options = Options()
for argument in ["--disable-blink-features=AutomationControlled", "start-maximized", "enable-automation", "--no-sandbox", "--disable-infobars", "--disable-dev-shm-usage", "--disable-browser-side-navigation", "--disable-gpu"]:
    options.add_argument(argument)
options.add_experimental_option('useAutomationExtension', "False")

driver = webdriver.Chrome(options= options)
driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                         'AppleWebKit/537.36 (KHTML, like Gecko) '
                         'Chrome/85.0.4183.102 Safari/537.36'})


# retrive website
end_date = datetime.strptime("2017-06-01", "%Y-%m-%d")
#end_date = datetime.strptime("2021-09-01", "%Y-%m-%d")
#news_types = ["bitcoin", "ripple", "ethereum", "litecoin", "altcoin"]
news_types = ["ripple"]
scrape_full_text = False


# iterate over news types and scrape all data from the page by scrolling down
for news_type in news_types:
    driver.get("https://cointelegraph.com/tags/{}".format(news_type))
    old_height = 1

    last_date = datetime.now()
    while len(driver.find_elements_by_xpath("//button[contains(@class, 'posts-listing__more-btn')]")) > 0 and last_date >= end_date:
        total_height = int(driver.execute_script("return document.body.scrollHeight"))
        for i in range(old_height, total_height - 1250, 25):
            driver.execute_script("window.scrollTo(0, {});".format(i))
        old_height = total_height
        next_btn = driver.find_elements_by_xpath("//button[contains(@class, 'posts-listing__more-btn')]")[0]
        next_btn.click()
        time.sleep(4)
        last_article = driver.find_element_by_xpath("(//article)[last()]")
        last_date = last_article.find_element_by_xpath(".//time[contains(@class, 'post-card-inline__date')]").text
        try:
            last_date = datetime.strptime(last_date, "%b %d, %Y")
        except:
            last_date = datetime.now()

        print(last_date.strftime("%Y-%m-%d"))

    articles = driver.find_elements_by_xpath("//article")

    data = pd.DataFrame()

    if not os.path.isfile("./data/articles_{}.csv".format(news_type)):
        for article in tqdm(articles, desc="Processing articles"):
            type = article.find_element_by_xpath(".//a[contains(@class, 'post-card-inline__figure-link')]").text
            link = article.find_element_by_xpath(".//a[contains(@class, 'post-card-inline__figure-link')]").get_attribute("href")
            header = article.find_element_by_xpath(".//a[contains(@class, 'post-card-inline__title-link')]").text
            text = article.find_element_by_xpath(".//div[contains(@class, 'post-card-inline__meta')]").text
            date = article.find_element_by_xpath(".//time[contains(@class, 'post-card-inline__date')]").text
            try:
                date = datetime.strptime(date, "%b %d, %Y")
            except:
                date = datetime.now()
            author =  article.find_element_by_xpath(".//p[contains(@class, 'post-card-inline__author')]").text
            if len(article.find_elements_by_xpath(".//div[contains(@class, 'post-card-inline__stats')]")) > 0:
                viewCount = int(article.find_element_by_xpath(".//div[contains(@class, 'post-card-inline__stats')]").text)
            else:
                viewCount = 0
            data = data.append(pd.Series({"type": type, "link": link, "header": header, "text": text, "date": date, "author": author, "viewCount": viewCount}), ignore_index= True)

        data.to_csv("./data/articles_{}.csv".format(news_type), index= False)
    else:
        data = pd.read_csv("./data/articles_{}.csv".format(news_type))

    if scrape_full_text:
        data["hashtags"] = np.nan
        for i in range(len(data["link"])):
            temp_link = data["link"].iloc[i]
            driver.get(temp_link)
            text = driver.find_element_by_xpath(".//article[contains(@class, 'post__article')]").text
            try:
                text = text.split("DELIVERED EVERY FRIDAY")[0]
            except:
                try:
                    text = text.split("RELATED NEWS")[0]
                except:
                    pass
            try:
                hashtags = [x.text.lower().replace("#","") for x in driver.find_elements_by_xpath(".//a[contains(@class, 'tags-list__link')]")]
            except:
                hashtags = []
            try:
                num_shares = int(driver.find_elements_by_xpath(".//span[contains(@class, 'post-actions__item-count')]")[1].text)
            except:
                num_shares = 0

            data["text"].iloc[i] = re.sub("/\s\s+/g" , " ", text.replace("\n", " "))
            data["hashtags"].iloc[i] = hashtags
            data["shares"] = num_shares

            time.sleep(abs(np.random.normal(1, 0.5)))

            if i % 300 == 0:
                rotate_VPN(google_check=1)

        data.to_csv("./data/articles_full_text_{}.csv".format(news_type), index=False)
        rotate_VPN(google_check=1)