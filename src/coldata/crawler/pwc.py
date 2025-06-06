import hashlib
import os
import requests
import time
import trafilatura
from bs4 import BeautifulSoup as bs
from tqdm import tqdm
from .crawler import Crawler
from ..utils import save, load


class PapersWithCode(Crawler):
    data_name = 'PapersWithCode'

    def __init__(self, database, website=None, **kwargs):
        super().__init__(self.data_name, database, website, **kwargs)
        self.init_page = website[self.data_name]['init_page']
        self.root_url = 'https://paperswithcode.com'
        self.datasets = self.make_datasets()
        self.num_datasets = len(self.datasets)

    def make_datasets(self):
        if self.num_attempts is not None and self.num_attempts == 0:
            datasets = []
            return datasets

        if self.use_cache and os.path.exists(os.path.join(self.cache_dir, 'datasets')):
            datasets = load(os.path.join(self.cache_dir, 'datasets'))
            return datasets

        response = requests.get(self.root_url + '/datasets')
        response.encoding = 'utf-8'
        soup = bs(response.content, 'html.parser')
        modality_div = soup.find('div', class_='filter-name', string=lambda t: t and 'Filter by Modality' in t)
        modality_section = modality_div.find_parent()
        modality_filters = modality_section.find_all('a', class_='filter-item')
        labels = [a.find(text=True, recursive=False).strip() for a in modality_filters]
        labels = [label.lower().replace(' ', '-') for label in labels]

        attempts_count = 0
        datasets = []
        last_result = None
        for label in labels:
            page = self.init_page
            while True:
                query_interval = self.query_interval
                try:
                    url = self.root_url + '/datasets/' + f'?mod={label}&page={page}'
                    print(f'Fetching page: {url}')
                    response = requests.get(url)
                    time.sleep(query_interval)
                    response.encoding = 'utf-8'
                    soup = bs(response.content, 'html.parser')
                    dataset_links = soup.select('a[href^="/dataset"]')
                    attempts_count += len(dataset_links)
                    result = []
                    for link in dataset_links:
                        if link['href'].split('/')[-1] != 'datasets':
                            result.append(link['href'])
                    datasets.extend(result)
                    attempts_count += len(result)
                    if last_result == tuple(result):
                        print('No datasets found on label {} and page {}.'.format(label, page))
                        break
                    else:
                        last_result = tuple(result)
                    if self.num_attempts is not None and attempts_count >= self.num_attempts:
                        break
                    page += 1
                except Exception as e:
                    print('Error fetching datasets on page {}: {}'.format(page, e))
                    query_interval = query_interval * self.query_interval_scaler
                    time.sleep(query_interval)
            if self.num_attempts is not None and attempts_count >= self.num_attempts:
                print('Reached the maximum number of attempts: {}'.format(self.num_attempts))
                break
        datasets = sorted(datasets, key=lambda x: x.split('/')[-1])
        save(datasets, os.path.join(self.cache_dir, 'datasets'))
        return datasets

    def make_data(self, url, soup):
        index = hashlib.sha256(url.encode()).hexdigest()
        data = {}
        data['website'] = 'Paper with Code'
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
