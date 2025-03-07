import hashlib
import os
import requests
import time
from bs4 import BeautifulSoup as bs
from tqdm import tqdm
from .crawler import Crawler
from .utils import clean_text, join_content
from ..utils import save, load


class PapersWithCode(Crawler):
    data_name = 'PapersWithCode'

    def __init__(self, database, website=None):
        super().__init__(self.data_name, database, website)
        self.init_page = website[self.data_name]['init_page']
        self.root_url = 'https://paperswithcode.com'
        self.datasets = self.make_datasets()
        self.num_datasets = len(self.datasets)

    def make_datasets(self):
        if self.use_cache and os.path.exists(os.path.join(self.cache_dir, 'datasets')):
            datasets = load(os.path.join(self.cache_dir, 'datasets'))
            return datasets

        attempts_count = 0
        self.page = self.init_page
        datasets = []
        while True:
            try:
                url = self.root_url + '/datasets/' + f'?page={self.page}'
                response = requests.get(url)
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
                if len(result) == 0:  # TODO: need to test termination
                    print('No datasets found on page {}.'.format(self.page))
                    break
                if self.num_attempts is not None and attempts_count >= self.num_attempts:
                    print('Reached the maximum number of attempts: {}'.format(self.num_attempts))
                    break
                self.page += 1
                time.sleep(self.query_interval)
            except Exception as e:
                print('Error fetching datasets on page {}: {}'.format(self.page, e))
                time.sleep(self.query_interval * 10)
        datasets = sorted(datasets, key=lambda x: x.split('/')[-1])
        save(datasets, os.path.join(self.cache_dir, 'datasets'))
        return datasets

    def make_data(self, url, soup):
        index = hashlib.sha256(url.encode()).hexdigest()
        data = {}
        data['website'] = 'Paper with Code'
        data['index'] = index
        data['URL'] = url
        # Parse datasets from soup
        elements = soup.find_all(['h1', 'p', 'a', 'footer', 'h4', 'h5'])
        current_group = {'header': None, 'content': []}
        cookie_keywords = ['cookie', 'privacy', 'consent', 'policy']
        if_first = True
        if_h4 = False
        for element in elements:
            if element.name == 'footer' or element.get('class') == ['footer'] or element.get('id') == 'footer':
                break
            if element.name == 'h1' and not if_h4:
                if current_group['header'] is not None:
                    if len(current_group['content']) > 0:
                        data[current_group['header'].strip().split('\n')[0]] = join_content(current_group['content'])
                header = element.get_text()
                current_group = {'header': header, 'content': []}
            elif element.name == 'h4':
                if_h4 = True
                if current_group['header'] is not None:
                    if len(current_group['content']) > 0:
                        if if_first:
                            data['Title'] = clean_text(current_group['header'])
                            data['Description'] = current_group['content'][2]
                            if_first = False
                        else:
                            data[current_group['header'].strip().split('\n')[0]] = join_content(
                                current_group['content'])
                header = element.get_text()
                current_group = {'header': header, 'content': []}
            elif element.name in ['p', 'a']:
                if 'https' in element.get_text():
                    continue
                content = element.get_text().strip().replace('Add a new result', '').replace(
                    'Link an existing benchmark', '').replace('Add', '').replace('Remove', '')
                if any(keyword.lower() in content.lower() for keyword in cookie_keywords):
                    continue
                current_group['content'].append(content)

        if current_group['header'] is not None:
            if len(current_group['content']) > 0:
                if if_first:
                    data['Title'] = clean_text(current_group['header'])
                    data['Description'] = join_content(current_group['content'])
                else:
                    data[current_group['header'].strip().split('\n')[0]] = join_content(current_group['content'][:-4])
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
            dataset = datasets[i]
            url_i = self.root_url + dataset
            page_i = requests.get(url_i)
            soup_i = bs(page_i.text, 'html.parser')
            data_i = self.make_data(url_i, soup_i)
            if is_upload:  # TODO :make a function
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
