import json
import hashlib
import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
from tqdm import tqdm
from .crawler import Crawler


class UCI(Crawler):
    data_name = 'UCI'

    def __init__(self, database, num_attempts=None, website=None):
        super().__init__(database)
        self.num_attempts = num_attempts
        self.num_datasets_per_query = website[self.data_name]['num_datasets_per_query']
        self.root_url = 'https://archive.ics.uci.edu'

    def make_url(self):
        url = set()
        URL = self.root_url + f'/datasets?skip=0&take={self.num_datasets_per_query}&sort=desc&orderBy=NumHits&search='
        page = requests.get(URL)
        soup = bs(page.content, 'html.parser')
        for h2 in soup.find_all('h2'):
            url.add(h2.find('a')['href'])
        url = list(url)
        return url

    def make_table(self, url):
        page = requests.get(self.root_url + url[0])
        soup = bs(page.text, 'html.parser')
        table = pd.DataFrame(columns=['index'] + ['Title', 'Description'] +
                                     [i.text for i in soup.find_all('h1')][1:7] + ['URL'])
        # table = pd.DataFrame(columns=['index'] + ['Title', 'Description'] +
        #                              [i.text for i in soup.find_all('h1')][1:] + ['URL'])
        return table

    def crawl(self):
        url = self.make_url()
        table = self.make_table(url)
        print(table)
        if self.num_attempts is not None:
            indices = range(self.num_attempts)
        else:
            indices = range(len(list(url)))
        print(f'Start crawling ({self.data_name})...')
        for i in tqdm(indices):
            url_i = self.root_url + url[i]
            page_i = requests.get(url_i)
            soup_i = bs(page_i.text, 'html.parser')
            index_i = hashlib.sha256(url_i.encode()).hexdigest()
            title_i = soup_i.find('h1').text
            info_i = [i.text for i in soup_i.find_all('p')]
            table.loc[len(table)] = [index_i] + [title_i] + info_i[:7] + [url_i]
            # table.loc[len(table)] = [index_i] + [title_i] + info_i + [url_i]
        data = table.to_json(orient='records')
        self.data = json.loads(data)
        return self.data

    def upload(self):
        count = 0
        print(f'Start uploading ({self.data_name})...')
        for data in tqdm(self.data):
            existing_data = self.database.collection.find_one({'index': data['index']})
            if existing_data is None:
                self.database.collection.insert_one(data)
                count += 1
        print(f'Insert {count} records.')
        return
