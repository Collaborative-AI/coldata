import hashlib
import os
import requests
import time
import trafilatura
from bs4 import BeautifulSoup as bs
from tqdm import tqdm
from .crawler import Crawler
from ..utils import save, load


class IEEEDataPort(Crawler):
    data_name = 'IEEEDataPort'

    def __init__(self, database, website=None, **kwargs):
        super().__init__(self.data_name, database, website, **kwargs)
        self.init_page = website[self.data_name]['init_page']
        self.root_url = 'https://ieee-dataport.org'
        self.categories = self.fetch_categories()
        self.datasets = self.make_datasets()
        self.num_datasets = len(self.datasets)

    def fetch_categories(self):
        resp = requests.get(f'{self.root_url}/datasets')
        resp.encoding = 'utf-8'
        soup = bs(resp.text, 'html.parser')
        tags = soup.select('a[href^="/topic-tags/"]')
        cats = sorted({a['href'].split('/')[2] for a in tags})
        return cats

    def make_datasets(self):
        if self.num_attempts is not None and self.num_attempts == 0:
            datasets = []
            return datasets

        if self.use_cache and os.path.exists(os.path.join(self.cache_dir, 'datasets')):
            datasets = load(os.path.join(self.cache_dir, 'datasets'))
            return datasets

        datasets = []
        attempts = 0
        for cat in self.categories:
            page = 0
            last = None
            while True:
                url = f'{self.root_url}/topic-tags/{cat}?page={page}'
                print(f'Fetching: {url}')
                resp = requests.get(url)
                resp.encoding = 'utf-8'
                soup = bs(resp.text, 'html.parser')
                links = soup.select('a[href^="/documents/"]')
                hrefs = [a['href'] for a in links]
                hrefs = list(dict.fromkeys(hrefs))  # unique preserve order

                if not hrefs or hrefs == last:
                    break
                datasets += hrefs
                last = hrefs
                attempts += len(hrefs)
                if self.num_attempts is not None and attempts >= self.num_attempts:
                    break
                page += 1
                time.sleep(self.query_interval)

            if self.num_attempts and attempts >= self.num_attempts:
                break

        datasets = sorted(datasets, key=lambda x: x.split('/')[-1])
        save(datasets, os.path.join(self.cache_dir, 'datasets'))
        return datasets

    def make_data(self, url, soup):
        index = hashlib.sha256(url.encode()).hexdigest()
        data = {}
        data['website'] = self.data_name
        data['index'] = index
        data['URL'] = url
        data['info'] = trafilatura.extract(str(soup), output_format=self.parse['output_format'])
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
            dataset = self.datasets[i]
            url_i = self.root_url + dataset
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
