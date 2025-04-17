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

    def __init__(self, database, website=None, **kwargs):
        super().__init__(self.data_name, database, website, **kwargs)
        self.root_url = 'https://registry.opendata.aws'
        self.datasets = self.make_datasets()
        self.num_datasets = len(self.datasets)

    def make_datasets(self):
        if self.num_attempts is not None and self.num_attempts == 0:
            datasets = []
            return datasets

        if self.use_cache and os.path.exists(os.path.join(self.cache_dir, 'datasets')):
            datasets = load(os.path.join(self.cache_dir, 'datasets'))
            return datasets

        while True:
            try:
                result = requests.get(self.root_url)
                result.encoding = 'utf-8'
                result.raise_for_status()  # Raise an HTTPError for bad responses (4xx, 5xx)
                break
            except Exception as e:
                print('Error fetching the page {}: {}'.format(self.root_url, e))
            time.sleep(self.query_interval)

        datasets = set()
        soup = bs(result.content, 'html.parser')
        for dataset in tqdm(soup.find_all('div', class_='dataset')):
            datasets.add(dataset.find('a')['href'])
        datasets = list(datasets)
        datasets = sorted(list(datasets), key=lambda x: x.split('/')[1])
        save(datasets, os.path.join(self.cache_dir, 'datasets'))
        return datasets

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
            indices = range(min(self.num_attempts, len(list(self.datasets))))
        else:
            indices = range(len(list(self.datasets)))
        print(f'Start crawling ({self.data_name})...')
        data = []
        for i in tqdm(indices):
            url_i = self.root_url + self.datasets[i]
            index_i = hashlib.sha256(url_i.encode()).hexdigest()
            existing_data = self.database.collection.find_one({'index': index_i})
            if existing_data is None:
                page_i = requests.get(url_i)
                soup_i = bs(page_i.text, 'html.parser')
                data_i = self.make_data(url_i, soup_i)
                if is_upload:
                    self._upload_data(data_i, self.verbose)
                else:
                    if self.query_interval > 0:
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
