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

    def make_url(self, num_datasets_per_query):
        url = set()
        URL = self.root_url + f'/datasets?skip=0&take={num_datasets_per_query}&sort=desc&orderBy=NumHits&search='
        page = requests.get(URL)
        soup = bs(page.content, 'html.parser')
        for h2 in soup.find_all('h2'):
            url.add(h2.find('a')['href'])
        url = list(url)
        return url

    def parse_soup(self, url, soup):
        feature = ['index'] + ['Title', 'Description'] + [i.text for i in soup.find_all('h1')] + ['URL']
        index = hashlib.sha256(url.encode()).hexdigest()
        title = soup.find('h1').text
        info = [i.text for i in soup.find_all('p')]
        data = [index] + [title] + info + [url]
        data = {feature: data[i] for i, feature in enumerate(feature)}
        return data

    def crawl(self):
        if self.num_attempts is not None:
            url = self.make_url(self.num_attempts)
            indices = range(self.num_attempts)
        else:
            url = self.make_url(self.num_datasets_per_query)
            indices = range(len(list(url)))
        print(f'Start crawling ({self.data_name})...')
        data = []
        for i in tqdm(indices):
            url_i = self.root_url + url[i]
            page_i = requests.get(url_i)
            soup_i = bs(page_i.text, 'html.parser')
            data_i = self.parse_soup(url_i, soup_i)
            data.append(data_i)
        self.data = data
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
