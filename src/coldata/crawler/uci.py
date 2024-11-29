import hashlib
import os
import requests
from bs4 import BeautifulSoup as bs
from tqdm import tqdm
from .crawler import Crawler
from .utils import clean_text, join_content
from ..utils import save, load


class UCI(Crawler):
    data_name = 'UCI'

    def __init__(self, database, website=None):
        super().__init__(self.data_name, database, website)
        self.num_datasets_per_query = website[self.data_name]['num_datasets_per_query']
        self.root_url = 'https://archive.ics.uci.edu'
        self.datasets = self.make_datasets()
        self.num_datasets = len(self.datasets)

    def make_datasets(self):
        if self.use_cache and os.path.exists(os.path.join(self.cache_dir, 'datasets')):
            datasets = load(os.path.join(self.cache_dir, 'datasets'))
            return datasets

        datasets = set()
        URL = self.root_url + f'/datasets?skip=0&take={self.num_datasets_per_query}&sort=desc&orderBy=NumHits&search='
        response = requests.get(URL)
        response.encoding = 'utf-8'
        soup = bs(response.content, 'html.parser')
        for h2 in soup.find_all('h2'):
            datasets.add(h2.find('a')['href'])
        datasets = sorted(list(datasets), key=lambda x: x.split('/')[-1])

        save(datasets, os.path.join(self.cache_dir, 'datasets'))
        return datasets

    def make_data(self, url, soup):
        index = hashlib.sha256(url.encode()).hexdigest()
        data = {}
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

    def crawl(self):
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
            data.append(data_i)
        self.data = data
        return self.data

    def upload(self):
        if not self.attempts_check():
            return
        count = 0
        print(f'Start uploading ({self.data_name})...')
        for data in tqdm(self.data):
            existing_data = self.database.collection.find_one({'index': data['index']})
            if existing_data is None:
                self.database.collection.insert_one(data)
                count += 1
        print(f'Insert {count} records.')
        return
