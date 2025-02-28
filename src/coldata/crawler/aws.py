import hashlib
import os
import requests
import time
from bs4 import BeautifulSoup as bs
from tqdm import tqdm
from .crawler import Crawler
from .utils import clean_text, join_content
from ..utils import save, load


class AWS(Crawler):
    data_name = 'AWS'

    def __init__(self, database, website=None):
        super().__init__(self.data_name, database, website)
        self.root_url = 'https://registry.opendata.aws'
        self.datasets = self.make_datasets()
        self.num_datasets = len(self.datasets)

    def make_datasets(self):
        if self.use_cache and os.path.exists(os.path.join(self.cache_dir, 'datasets')):
            datasets = load(os.path.join(self.cache_dir, 'datasets'))
            return datasets

        url = set()
        while True:
            try:
                response = requests.get(self.root_url)
                response.raise_for_status()  # Raise an HTTPError for bad responses (4xx, 5xx)
                response.encoding = 'utf-8'
                break
            except Exception as e:
                print('Error fetching the page {}: {}'.format(self.root_url, e))
            time.sleep(self.query_interval)

        soup = bs(response.content, 'html.parser')
        datasets = soup.find_all('div', class_='dataset')
        for dataset in tqdm(datasets):
            url.add(dataset.find('a')['href'])
        url = list(url)
        save(url, os.path.join(self.cache_dir, 'datasets'))
        return url

    def make_data(self, url, soup):
        index = hashlib.sha256(url.encode()).hexdigest()
        data = {}
        data['website'] = 'AWS'
        data['index'] = index
        data['URL'] = url

        elements = soup.find_all(['h1', 'p', 'a', 'h4', 'h5', 'h3'])

        # Initialize variables for storing results
        current_group = {'header': None, 'content': []}
        footer = False
        if_first = True
        # Iterate through each element
        for element in elements:
            if element.name == 'h1' or element.name == 'h4':  # If it's a header
                if current_group['header'] is not None:
                    if len(current_group['content']) > 0:
                        if if_first:
                            data['title'] = clean_text(current_group['header'])
                            data['keywords'] = current_group['content'][0].strip().replace('\n', ',')
                            data['description'] = current_group['content'][1]
                            if_first = False
                        else:
                            data[current_group['header']] = join_content(current_group['content'])
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
                    data['title'] = clean_text(current_group['header'])
                    data['keywords'] = current_group['content'][0].strip().replace('\n', ',')
                    data['description'] = current_group['content'][1]
                else:
                    data[current_group['header']] = join_content(current_group['content'])
        return data

    def crawl(self, is_upload=False):
        if not self.attempts_check():
            return
        if self.num_attempts is not None:
            datasets = self.datasets[:self.num_attempts]
            indices = range(self.num_attempts)
        else:
            datasets = self.datasets
            indices = range(len(list(datasets)))
        print(f'Start crawling ({self.data_name})...')
        data = []
        for i in tqdm(indices):
            url_i = self.root_url + datasets[i]
            page_i = requests.get(url_i)
            soup_i = bs(page_i.text, 'html.parser')
            data_i = self.make_data(url_i, soup_i)
            if is_upload:
                is_insert = self._upload_data(data_i, self.verbose)
            else:
                is_insert = False
            if is_insert and self.query_interval > 0:
                time.sleep(self.query_interval)
            data.append(data_i)
        return data

    def upload(self, data):
        if not self.attempts_check():
            return
        count = 0
        print('Start uploading ({})...'.format(self.data_name))
        for data_i in tqdm(data):
            is_insert = self._upload_data(data_i, self.verbose)
            if is_insert:
                count += 1
        print('Insert {} records.'.format(count))
        return
