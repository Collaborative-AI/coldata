import json
import hashlib
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
        self.num_attempts = num_attempts
        self.root_url = 'https://registry.opendata.aws'

    def make_url(self):
        url = set()
        page = requests.get(self.root_url)
        soup = bs(page.content, 'html.parser')
        datasets = soup.find_all('div', class_='dataset')
        for dataset in tqdm(datasets):
            url.add(dataset.find('a')['href'])
        url = list(url)
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
        url = self.make_url()
        if self.num_attempts is not None:
            indices = range(self.num_attempts)
        else:
            indices = range(len(list(url)))
        data = []
        print(f'Start crawling ({self.data_name})...')
        for i in tqdm(indices):
            url_i = self.root_url + url[i]
            page_i = requests.get(url_i)
            soup_i = bs(page_i.text, 'html.parser')
            data_i = self.make_data(url_i, soup_i)
            data.append(data_i)
        print(data)
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
