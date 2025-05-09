import time
import hashlib
import os
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

    def __init__(self, database, website=None, selenium=None, **kwargs):
        super().__init__(self.data_name, database, website, **kwargs)
        self.root_url = 'https://opendatalab.com'
        self.init_page = website[self.data_name]['init_page']
        self.num_datasets_per_query = website[self.data_name]['num_datasets_per_query']
        self.chromedriver_path = selenium.get('chromedriver_path')
        self.driver = self._initialize_driver()
        self.datasets = self.make_datasets()
        self.num_datasets = len(self.datasets)

    def _initialize_driver(self):
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        service = Service(self.chromedriver_path)
        driver = webdriver.Chrome(service=service, options=options)
        return driver

    def make_datasets(self):
        if self.num_attempts is not None and self.num_attempts == 0:
            datasets = []
            return datasets

        if self.use_cache and os.path.exists(os.path.join(self.cache_dir, 'datasets')):
            datasets = load(os.path.join(self.cache_dir, 'datasets'))
            return datasets

        attempts_count = 0
        datasets = []
        url = f'{self.root_url}/?pageNo=1&pageSize={self.num_datasets_per_query}&sort=all'
        self.driver.get(url)
        time.sleep(5)
        soup = bs(self.driver.page_source, 'html.parser')
        pagination_items = soup.find_all('li', class_='ant-pagination-item')
        last_page = int(list(pagination_items)[-1].get('title'))

        for page in range(self.init_page, last_page + 1):
            try:
                url = f'{self.root_url}/?pageNo={page}&pageSize={self.num_datasets_per_query}&sort=all'
                print(f'Fetching page: {url}')
                self.driver.get(url)
                time.sleep(self.query_interval)
                soup = bs(self.driver.page_source, 'html.parser')
                cards = soup.find_all('a', class_='_cardContainer_1vhh8_1')
                result = []
                for card in cards:
                    href = card.get('href')
                    if href:
                        result.append(self.root_url + href if not href.startswith('http') else href)
                datasets.extend(result)
                attempts_count += len(result)
                if self.num_attempts is not None and attempts_count >= self.num_attempts:
                    break
            except Exception as e:
                print('Error fetching datasets on page {}: {}'.format(page, e))
                time.sleep(self.query_interval * self.query_interval_scaler)
            if self.num_attempts is not None and attempts_count >= self.num_attempts:
                print('Reached the maximum number of attempts: {}'.format(self.num_attempts))
                break
        datasets = sorted(datasets, key=lambda x: x.split('/')[-1])
        save(datasets, os.path.join(self.cache_dir, 'datasets'))
        return datasets

    def make_data(self, url):
        # Load the page content
        self.driver.get(url)
        time.sleep(self.query_interval)
        soup = bs(self.driver.page_source, 'html.parser')

        # Tags to match
        allowed_tags = ['h1', 'h2', 'h3', 'h4', 'h5', 'p', 'li', 'a', 'code', 'div']
        # Class constraint for divs
        header_target_div_classes = ['font-semibold']
        content_target_div_classes = ['ant-space-item', 'mr-24', 'mb-6']
        target_div_classes = header_target_div_classes + content_target_div_classes
        # Iterate over all elements in document order
        elements = []
        for tag in soup.descendants:
            if not hasattr(tag, 'name'):
                continue  # Skip text nodes and comments
            if tag.name not in allowed_tags:
                continue
            if tag.name == 'div':
                if tag.has_attr('class') and \
                        any(target_div_class in tag['class'] for target_div_class in target_div_classes):
                    elements.append(tag)
            else:
                elements.append(tag)
        index = hashlib.sha256(url.encode()).hexdigest()
        data = {}
        data['index'] = index
        data['URL'] = url

        cookie_keywords = ['cookie', 'privacy', 'consent', 'policy']
        footer_keywords = ['© 2022 OpenDatalab. All Rights Reserved.', '沪ICP备2021009351号-5', '免责申明']
        # Initialize variables for storing results
        current_group = {'header': None, 'content': []}
        if_first = True

        for element in elements:
            if element.name in ['h1', 'h2', 'h3', 'h4', 'h5'] or \
                    (element.name == 'div' and
                     any(target_div_class in element['class'] for target_div_class in header_target_div_classes)):
                if current_group['header'] is not None:
                    if if_first:
                        data['title'] = clean_text(current_group['header'])
                        data['description'] = clean_text(join_content(current_group['content']))
                        if_first = False
                    else:
                        data[current_group['header']] = join_content(current_group['content'])
                header = element.get_text()
                if any(keyword.lower() in header.lower() for keyword in cookie_keywords + footer_keywords):
                    continue
                current_group = {'header': header, 'content': []}
            else:
                content = element.get_text()
                if any(keyword.lower() in content.lower() for keyword in cookie_keywords + footer_keywords):
                    continue
                current_group['content'].append(content)

        if current_group['header'] is not None:
            if len(current_group['content']) > 0:
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
            url_i = self.datasets[i]
            index_i = hashlib.sha256(url_i.encode()).hexdigest()
            existing_data = self.database.collection.find_one({'index': index_i})
            if existing_data is None:
                data_i = self.make_data(url_i)
                if is_upload:
                    self._upload_data(data_i, self.verbose)
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
