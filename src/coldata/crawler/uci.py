import hashlib
import os
import requests
import time
from bs4 import BeautifulSoup as bs
from tqdm import tqdm
from .crawler import Crawler
from .utils import clean_text, join_content
from ..utils import save, load


class UCI(Crawler):
    data_name = 'UCI'

    def __init__(self, database, website=None, **kwargs):
        super().__init__(self.data_name, database, website, **kwargs)
        self.num_datasets_per_query = website[self.data_name]['num_datasets_per_query']
        self.root_url = 'https://archive.ics.uci.edu'
        self.datasets = self.make_datasets()
        self.num_datasets = len(self.datasets)

    def make_datasets(self):
        if self.num_attempts is not None and self.num_attempts == 0:
            datasets = []
            return datasets

        if self.use_cache and os.path.exists(os.path.join(self.cache_dir, 'datasets')):
            datasets = load(os.path.join(self.cache_dir, 'datasets'))
            return datasets

        url = self.root_url + f'/datasets?skip=0&take={self.num_datasets_per_query}&sort=desc&orderBy=NumHits&search='
        while True:
            try:
                response = requests.get(url)
                response.encoding = 'utf-8'
                response.raise_for_status()  # Raise an HTTPError for bad responses (4xx, 5xx)
                break
            except Exception as e:
                print('Error fetching the page {}: {}'.format(url, e))
            time.sleep(self.query_interval)

        datasets = set()
        soup = bs(response.content, 'html.parser')
        for h2 in tqdm(soup.find_all('h2')):
            datasets.add(h2.find('a')['href'])
        datasets = sorted(list(datasets), key=lambda x: x.split('/')[-1])
        save(datasets, os.path.join(self.cache_dir, 'datasets'))
        return datasets

    def make_data(self, url, soup):
        index = hashlib.sha256(url.encode()).hexdigest()
        data = {}
        data['website'] = 'UCI'
        data['index'] = index
        data['URL'] = url

        # Find all headers and paragraphs in the order they appear
        elements = soup.find_all(['h1', 'p', 'a', 'footer'])
        cookie_keywords = ["cookie", "privacy", "consent", "policy"]

        # Initialize variables for storing results
        current_group = {'header': None, 'content': []}

        if_first = True
        # Iterate through each element
        for element in elements:
            if element.name == 'footer' or element.get('class') == ['footer'] or element.get('id') == 'footer':
                break  # Exit the loop as we don't need to process elements beyond the footer
            if element.name == 'h1':  # If it's a header
                if current_group['header'] is not None:
                    if len(current_group['content']) > 0:
                        if if_first:
                            data['title'] = clean_text(current_group['header'])
                            data['description'] = join_content(current_group['content'])
                            if_first = False
                        else:
                            data[current_group['header']] = join_content(current_group['content'])
                header = element.get_text()
                current_group = {'header': header, 'content': []}
            elif element.name in ['p', 'a']:  # If it's a paragraph or a link
                content = element.get_text()
                # Check for cookie consent keywords and skip if found
                if any(keyword.lower() in content.lower() for keyword in cookie_keywords):
                    continue  # Skip paragraphs with cookie consent keywords

                current_group['content'].append(content)

        # Add the last group after the loop ends
        if current_group['header'] is not None:
            if len(current_group['content']) > 0:
                if if_first:
                    data['title'] = clean_text(current_group['header'])
                    data['description'] = join_content(current_group['content'])
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
