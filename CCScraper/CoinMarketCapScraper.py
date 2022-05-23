import pandas as pd
import numpy as np
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from tqdm import tqdm
from nordvpn_switcher import initialize_VPN, rotate_VPN
import time
from os import listdir
from os.path import isfile, join

options = Options()
#for argument in ["--disable-blink-features=AutomationControlled", "start-maximized", "enable-automation", "--headless", "--no-sandbox", "--disable-infobars", "--disable-dev-shm-usage", "--disable-browser-side-navigation", "--disable-gpu"]:
for argument in ["--disable-blink-features=AutomationControlled", "start-maximized", "enable-automation", "--no-sandbox", "--disable-infobars", "--disable-dev-shm-usage", "--disable-browser-side-navigation", "--disable-gpu"]:
    options.add_argument(argument)
options.add_experimental_option('useAutomationExtension', "False")

# initialize selenium
driver = webdriver.Chrome(options= options)
driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                         'AppleWebKit/537.36 (KHTML, like Gecko) '
                         'Chrome/85.0.4183.102 Safari/537.36'})

# initialize
initialize_VPN(save=1, area_input=['random countries europe 25'])
rotate_VPN(google_check = 1)
time.sleep(5)

scrape_links = False
scrape_data = True

#retrieve the links of the ccs which should be scraped
if scrape_links:
    links = []
    for i in tqdm(range(1, 51)):
        driver.get("https://coinmarketcap.com/?page={}".format(i))

        total_height = int(driver.execute_script("return document.body.scrollHeight"))
        for i in range(1, total_height, 10):
            driver.execute_script("window.scrollTo(0, {});".format(i))

        table = driver.find_elements_by_xpath("//table")[0]
        temp = [x.get_attribute("href") for x in table.find_elements_by_xpath("//tbody/tr/td[3]/div/a")]
        links.extend(temp)

        if i % 10 == 0 and i != 0:
            driver = webdriver.Chrome(options=options)
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            driver.execute_cdp_cmd('Network.setUserAgentOverride',
                                   {"userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                                                 'AppleWebKit/537.36 (KHTML, like Gecko) '
                                                 'Chrome/85.0.4183.102 Safari/537.36'})
            rotate_VPN(google_check = 1)

    links = pd.DataFrame(links, columns= ["Links"])
    links.to_csv("./data/links.csv")
else:
    links = pd.read_csv("./data/missing_links.csv", index_col=0)

# only scrape the top 500 currencies
#num_processed = len([f for f in listdir("./data/cc") if isfile(join("./data/cc", f))])
#links = links.iloc[:(500-num_processed), :]

# scrape the link retrieved before
i = 0
if scrape_data:
    for link in tqdm(links["Links"]):
        # example link for debugging
        #link = "https://coinmarketcap.com/currencies/dash-cash/"
        driver.get(link + "historical-data/")

        codeName = driver.find_element_by_xpath("//div[contains(concat(' ', @class, ' '), 'nameHeader')]").text.split("\n")
        code = codeName[1]
        name = codeName[0]

        try:
            has_cookie_banner = driver.find_element_by_xpath("//div[contains(@class, 'cmc-cookie-policy-banner__close')]").is_displayed()
        except:
            has_cookie_banner = False
        if has_cookie_banner:
            driver.find_element_by_xpath("//div[contains(@class, 'cmc-cookie-policy-banner__close')]").click()

        init_total_height = int(driver.execute_script("return document.body.scrollHeight")) - 800
        total_height = init_total_height
        total_moved = 0
        while total_height > 0:
            moveDown = int(np.random.normal(60, 10))
            total_moved += moveDown
            driver.execute_script("window.scrollTo(0, {});".format(int(total_moved)))
            total_height -= moveDown
            time.sleep(0.05)
            if not int(driver.execute_script("return document.body.scrollHeight")) - 800  in range(init_total_height-20, init_total_height+20):
                time.sleep(1)
                total_height = int(driver.execute_script("return document.body.scrollHeight")) - 800 - total_moved
                init_total_height = int(driver.execute_script("return document.body.scrollHeight")) - 800
        time.sleep(3)

        previous_position = -1
        retries = 0
        while retries < 5:
            try:
                while previous_position != driver.execute_script("return window.pageYOffset;"):
                    previous_position = driver.execute_script("return window.pageYOffset;")
                    driver.find_element_by_xpath("//button[contains(@class, 'DChGS')]").click()
                    i += 1
                    sleepTime = np.random.normal(3, 0.4)
                    time.sleep(sleepTime)

                    init_total_height = int(driver.execute_script("return document.body.scrollHeight")) - 800
                    total_height = init_total_height - total_moved
                    while total_height > 0:
                        moveDown = int(np.random.normal(60, 10))
                        total_moved += moveDown
                        driver.execute_script("window.scrollTo(0, {});".format(int(total_moved)))
                        total_height -= moveDown
                        time.sleep(0.1)
                        if not int(driver.execute_script("return document.body.scrollHeight")) - 800  in range(init_total_height-20, init_total_height+20):
                            time.sleep(1)
                            total_height = int(driver.execute_script("return document.body.scrollHeight")) - 800 - total_moved
                            init_total_height = int(driver.execute_script("return document.body.scrollHeight")) - 800

                    if i % 100 == 0 and i != 0:
                        driver.delete_all_cookies()
                        rotate_VPN(google_check=1)
                        time.sleep(5)
            except:
                pass

            retries += 1
            time.sleep(15)
            driver.execute_script("window.scrollTo(0, {});".format(int(driver.execute_script("return document.body.scrollHeight")) - 800))
            previous_position -= 1

        header = [x.text for x in driver.find_elements_by_xpath("//table/thead/tr/th")]
        content = [x.text for x in driver.find_elements_by_xpath("//table/tbody/tr/td")]
        content = np.reshape(content, (-1, len(header)))

        pd.DataFrame(content, columns= header).to_csv("./data/cc/{}_{}.csv".format(code, name), index= False)

        driver.delete_all_cookies()
