import json
import hashlib
import os
import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
from tqdm import tqdm
from .crawler import Crawler
from .utils import clean_text, join_content
from ..utils import save, load


class AWS(Crawler):
    data_name = 'AWS'

    def __init__(self, database, website=None):
        super().__init__(self.data_name, database, website)
        self.num_datasets_per_query = website[self.data_name]['num_datasets_per_query']
        self.root_url = 'https://registry.opendata.aws'
        self.datasets = self.make_datasets()
        self.num_datasets = len(self.datasets)

    def make_datasets(self):
        if self.use_cache and os.path.exists(os.path.join(self.cache_dir, 'datasets')):
            datasets = load(os.path.join(self.cache_dir, 'datasets'))
            return datasets
        
        url = set()
        page = requests.get(self.root_url)
        page.encoding = 'utf-8'
        soup = bs(page.content, 'html.parser')
        datasets = soup.find_all('div', class_='dataset')
        for dataset in tqdm(datasets):
            url.add(dataset.find('a')['href'])
        url = list(url)
        save(url, os.path.join(self.cache_dir, 'datasets'))
        return url

    def make_data(self, url, soup):
        index = hashlib.sha256(url.encode()).hexdigest()
        data = {}
        data['index'] = index
        data['URL'] = url
        data = self.parse_soup(soup, data)
        return data

    def parse_soup(self, soup_i, parsed):
        elements = soup_i.find_all(['h1', 'p', 'a', 'h4', 'h5', 'h3'])

        # Initialize variables for storing results
        current_group = {'header': None, 'content': []}
        footer = False
        if_first = True
        # Iterate through each element
        for element in elements:
            if element.name == 'h1' or element.name =='h4':  # If it's a header
                if current_group['header'] is not None:
                    if len(current_group['content']) > 0:
                        if if_first:
                            parsed['Title'] = clean_text(current_group['header'])
                            parsed['Labels'] = current_group['content'][0].strip().replace('\n', ',')
                            parsed['Description'] = current_group['content'][1]
                            if_first = False
                        else:
                            parsed[current_group['header']] = join_content(current_group['content'])
                header = element.get_text()
                current_group = {'header': header, 'content': []}
            elif element.name in ['p', 'a', 'h5']:  # If it's a paragraph or a link
                content = element.get_text()

                current_group['content'].append(content)
            else:
                if footer:
                    break
                footer = True
        if current_group['header'] is not None:
            if len(current_group['content']) > 0:
                if if_first:
                    parsed['Title'] = clean_text(current_group['header'])
                    parsed['Labels'] = current_group['content'][0].strip().replace('\n', ',')
                    parsed['Description'] = current_group['content'][1]
                else:
                    parsed[current_group['header']] = join_content(current_group['content'])
        return parsed
        
    def crawl(self):
        if not self.attempts_check():
            return
        if self.num_attempts is not None:
            print("here")
            datasets = self.datasets[:self.num_attempts]
            indices = range(self.num_attempts)
        else:
            print("here")
            datasets = self.datasets
            indices = range(len(list(datasets)))
        data = []
        print(f'Start crawling ({self.data_name})...')
        for i in tqdm(indices):
            url_i = self.root_url + datasets[i]
            page_i = requests.get(url_i)
            soup_i = bs(page_i.text, 'html.parser')
            data_i = self.make_data(url_i, soup_i)
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
