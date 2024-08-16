import json
import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
from tqdm import tqdm
from .crawler import Crawler


class UCI(Crawler):
    def __init__(self, mongodb_key_path, attempts=None, max_num_datasets=1000):
        super().__init__(mongodb_key_path, attempts)
        self.data_name = 'UCI'
        self.max_num_datasets = max_num_datasets
        self.root_url = 'https://archive.ics.uci.edu'

    def make_url(self):
        url = set()
        URL = self.root_url + f'/datasets?skip=0&take={self.max_num_datasets}&sort=desc&orderBy=NumHits&search='
        page = requests.get(URL)
        soup = bs(page.content, 'html.parser')
        for h2 in soup.find_all('h2'):
            url.add(h2.find('a')['href'])
        url = list(url)
        return url

    def make_table(self, url):
        page = requests.get(self.root_url + url[0])
        soup = bs(page.text, 'html.parser')
        table = pd.DataFrame(columns=['Title', 'Description'] + [i.text for i in soup.find_all('h1')][1:7] + ['URL'])
        return table

    def crawl(self):
        url = self.make_url()
        table = self.make_table(url)
        if self.attempts is not None:
            indices = range(self.attempts)
        else:
            indices = range(len(list(url)))
        print(f'Start crawling ({self.data_name})...')
        for i in tqdm(indices):
            url_i = url[i]
            page_i = requests.get(self.root_url + url_i)
            soup_i = bs(page_i.text, 'html.parser')
            table.loc[len(table)] = [soup_i.find('h1').text] + [i.text for i in soup_i.find_all('p')][:7] + \
                                    [self.root_url + url_i]
        self.data = json.loads(table.to_json(orient='records'))
        return self.data

    def upload(self):
        count = 0
        print(f'Start uploading ({self.data_name})...')
        for data in tqdm(self.data):
            existing_data = self.collection.find_one({'URL': data['URL']})
            if existing_data is None:
                self.collection.insert_one(data)
                count += 1
        print(f'Insert {count} number of data')
        return
