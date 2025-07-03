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
        self.base_url = 'https://ieee-dataport.org'
        self.categories = self._fetch_categories()
        self.datasets = self._make_datasets()
        self.num_datasets = len(self.datasets)

    def _fetch_categories(self):
        resp = requests.get(f'{self.base_url}/datasets')
        resp.encoding = 'utf-8'
        soup = bs(resp.text, 'html.parser')
        tags = soup.select('a[href^="/topic-tags/"]')
        cats = sorted({a['href'].split('/')[2] for a in tags})
        return cats

    def _make_datasets(self):
        cache_file = os.path.join(self.cache_dir, 'ieee_datasets')
        if self.use_cache and os.path.exists(cache_file):
            return load(cache_file)

        datasets = []
        attempts = 0
        for cat in self.categories:
            page = 0
            last = None
            while True:
                url = f'{self.base_url}/topic-tags/{cat}?page={page}'
                print(f'Fetching: {url}')
                resp = requests.get(url)
                resp.encoding = 'utf-8'
                soup = BeautifulSoup(resp.text, 'html.parser')
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

        datasets = sorted(set(datasets))
        save(datasets, cache_file)
        return datasets

    def make_data(self, url):
        index = hashlib.sha256(url.encode()).hexdigest()
        resp = requests.get(url)
        resp.encoding = 'utf-8'
        soup = bs(resp.text, 'html.parser')

        data = {
            'website': 'IEEE DataPort',
            'index': index,
            'URL': url,
            'title': soup.select_one('h1.page-title').get_text(strip=True),
            'description': trafilatura.extract(str(soup), output_format=self.parse['output_format']),
            'category': soup.select_one('li.topic a').get_text(strip=True) if soup.select_one('li.topic a') else None,
        }
        return data

    def crawl(self, is_upload=False):
        if not self.attempts_check():
            return
        maxi = self.num_attempts or len(self.datasets)
        print(f'Start crawling {self.data_name} (max {maxi})')
        results = []
        for href in tqdm(self.datasets[:maxi]):
            full_url = self.base_url + href
            idx = hashlib.sha256(full_url.encode()).hexdigest()
            if self.database.collection.find_one({'index': idx}):
                continue
            try:
                data = self.make_data(full_url)
            except Exception as e:
                print(f'Error fetching {full_url}: {e}')
                continue
            if is_upload:
                self._upload_data(data, self.verbose)
                time.sleep(self.query_interval)
            results.append(data)
        return results

    def upload(self, data_list):
        if not self.attempts_check():
            return
        count = 0
        print(f'Uploading {len(data_list)} items for {self.data_name}')
        for item in tqdm(data_list):
            if self._upload_data(item, self.verbose):
                count += 1
        print(f"Inserted {count} new records.")
        return
