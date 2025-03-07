import time
import hashlib
import os
import requests
from bs4 import BeautifulSoup as bs
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from .crawler import Crawler
from .utils import clean_text, join_content
from ..utils import save, load

class OpenDataLab(Crawler):
    data_name = 'OpenDataLab'

    def __init__(self, database, website=None, selenium=None):
        super().__init__(self.data_name, database, website)
        self.root_url = 'https://opendatalab.com'
        self.driver_path = selenium.get("chromedriver_path")
        self.driver = self._initialize_driver()
        self.num_datasets_per_query = website[self.data_name]['num_datasets_per_query']
        self.datasets = self.make_datasets()
        self.num_datasets = len(self.datasets)

    def _initialize_driver(self):
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        service = Service(self.driver_path)
        return webdriver.Chrome(service=service, options=options)

    def make_datasets(self, page_no=1):
        if self.use_cache and os.path.exists(os.path.join(self.cache_dir, 'datasets')):
            datasets = load(os.path.join(self.cache_dir, 'datasets'))
            return datasets

        datasets = set()
        url = f"{self.root_url}/?pageNo={page_no}&pageSize={self.num_datasets_per_query}&sort=all"
        print(f"Fetching page: {url}")
        self.driver.get(url)
        time.sleep(5)  # Wait for JavaScript to load the content
        soup = bs(self.driver.page_source, 'html.parser')

        # Extract dataset links
        cards = soup.find_all('a', class_='_cardContainer_1vhh8_1')
        for card in cards:
            href = card.get('href')
            if href:
                datasets.add(self.root_url + href if not href.startswith('http') else href)
        datasets = sorted(list(datasets), key=lambda x: x.split('/')[-1])
        save(datasets, os.path.join(self.cache_dir, 'datasets'))
        return datasets

    def make_data(self, url, soup):
        # Load the page content
        self.driver.get(url)
        time.sleep(5)
        soup = bs(self.driver.page_source, 'html.parser')
        index = hashlib.sha256(url.encode()).hexdigest()
        data = {}
        data['index'] = index
        data['URL'] = url

        elements = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'p', 'li', 'a'])
        cookie_keywords = ["cookie", "privacy", "consent", "policy"]
        footer_keywords = ["© 2022 OpenDatalab. All Rights Reserved.", "沪ICP备2021009351号-5", "Similar Datasets"]

        # Initialize variables for storing results
        current_group = {'header': None, 'content': []}
        if_first = True

        for element in elements:
            if element.name in ['h1', 'h2', 'h3', 'h4', 'h5']:
                if current_group['header'] is not None:
                    if len(current_group['content']) > 0:
                        if if_first:
                            data['Title'] = clean_text(current_group['header'])
                            data['Description'] = join_content(current_group['content'])
                            if_first = False
                        else:
                            data[current_group['header']] = join_content(current_group['content'])
                header = element.get_text()
                current_group = {'header': header, 'content': []}
            else:
                content = element.get_text()
                # Check for cookie consent keywords and skip if found
                if any(keyword.lower() in content.lower() for keyword in cookie_keywords + footer_keywords):
                    continue
                current_group['content'].append(content)

        if current_group['header'] is not None:
            if len(current_group['content']) > 0:
                if if_first:
                    data['Title'] = clean_text(current_group['header'])
                    data['Description'] = join_content(current_group['content'])
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
            #url_i = self.root_url + datasets[i]
            url_i = datasets[i] if datasets[i].startswith("http") else f"{self.root_url}/{datasets[i]}"
            print(f"Fetching dataset page: {url_i}")
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
