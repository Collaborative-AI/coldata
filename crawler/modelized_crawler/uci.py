from modelized_crawler.crawler import Crawler
import requests
import numpy
import pandas as pd
from bs4 import BeautifulSoup as bs
from tqdm import tqdm
import json


class UCI(Crawler):

    def __init__(self):
        super().__init__()

    def process_data(self):
        urls = set()
        URL = "https://archive.ics.uci.edu/datasets?skip=0&take=1000&sort=desc&orderBy=NumHits&search="  # assuming uci archive size no more than 1k
        page = requests.get(URL)
        soup = bs(page.content, "html.parser")
        for h2 in soup.find_all('h2'):
            urls.add(h2.find('a')['href'])
        df = pd.DataFrame(columns=["id", "url"])
        page0 = requests.get("https://archive.ics.uci.edu" + list(urls)[0])
        soup0 = bs(page0.text, "html.parser")
        tb = pd.DataFrame(columns=["Title", "Description"] + [i.text for i in soup0.find_all('h1')][1:7])
        for url in tqdm(list(urls)):
            df.loc[len(df)] = [url[url.rfind("/") + 1:], "https://archive.ics.uci.edu" + url]
            page0 = requests.get("https://archive.ics.uci.edu" + url)
            soup0 = bs(page0.text, "html.parser")
            txt2 = soup0.find_all(['h1', 'p'])
            tb.loc[len(tb)] = [soup0.find('h1').text] + [i.text for i in soup0.find_all('p')][:7]
        self.dt = json.loads(pd.concat([df, tb], axis=1).to_json(orient="records"))

    def upload_data(self):
        count = 0
        for data in self.dt:
            # exists duplicate names, but different datasets
            existing_data = self.collection.find_one({"url": data["url"]})
            if existing_data is None:
                # Data is not in the collection, so insert it
                self.collection.insert_one(data)
                count += 1
        print('Inserted', count, "number of data")
