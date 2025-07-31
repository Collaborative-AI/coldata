import hashlib
import os
import requests
import time
import trafilatura
from bs4 import BeautifulSoup as bs
from tqdm import tqdm
from .crawler import Crawler
from ..utils import save, load


class BrainDataSciencePlatform(Crawler):
    data_name = 'BrainDataSciencePlatform'

    def __init__(self, database, website=None, **kwargs):
        super().__init__(self.data_name, database, website, **kwargs)
        self.root_url = 'https://bdsp.io'
        self.datasets = self.make_datasets()
        self.num_datasets = len(self.datasets)

    def make_datasets(self):
        if self.num_attempts is not None and self.num_attempts == 0:
            return []

        cache_path = os.path.join(self.cache_dir, 'datasets')
        if self.use_cache and os.path.exists(cache_path):
            return load(cache_path)

        list_url = f'{self.root_url}/about/database/'
        print(f'Fetching dataset list from {list_url}...')
        try:
            resp = requests.get(list_url)
            resp.encoding = 'utf‑8'
            resp.raise_for_status()
        except Exception as e:
            print(f'Error fetching page: {e}')
            return []

        soup = bs(resp.text, 'html.parser')
        datasets = set()

        # find <a> links to content pages under /content/
        for a in soup.select('a[href^="/content/"]'):
            href = a['href']
            # Ensure format like '/content/<slug>/' or versioned '/content/<slug>/1.0/'
            if href.count('/') >= 2:
                datasets.add(href.rstrip('/'))

        datasets = sorted(datasets)
        save(datasets, cache_path)
        return datasets

    def make_data(self, url, soup):
        index = hashlib.sha256(url.encode()).hexdigest()
        return {
            'website': self.data_name,
            'index': index,
            'URL': url,
            'info': trafilatura.extract(str(soup), output_format=self.parse['output_format'])
        }

    def crawl(self, is_upload=False):
        if not self.attempts_check():
            return

        indices = range(min(self.num_attempts, len(self.datasets))) if self.num_attempts else range(len(self.datasets))
        print(f'Start crawling ({self.data_name})...')
        collected = []

        for i in tqdm(indices):
            rel = self.datasets[i]
            full = f'{self.root_url}{rel}/'
            idx = hashlib.sha256(full.encode()).hexdigest()

            if self.database.collection.find_one({'index': idx}):
                continue

            try:
                resp = requests.get(full)
                resp.encoding = 'utf‑8'
                resp.raise_for_status()
                soup = bs(resp.text, 'html.parser')
                data_i = self.make_data(full, soup)
                if is_upload:
                    self._upload_data(data_i, self.verbose)
                else:
                    collected.append(data_i)
                if self.query_interval > 0:
                    time.sleep(self.query_interval)
            except Exception as e:
                print(f'Failed to fetch {full}: {e}')
                continue

        return collected

    def upload(self, data):
        if not self.attempts_check():
            return
        print(f'Uploading ({self.data_name})...')
        count = 0
        for data_i in tqdm(data):
            if self._upload_data(data_i, self.verbose):
                count += 1
        print(f'Inserted {count} records.')
        return
