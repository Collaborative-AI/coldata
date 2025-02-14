
import os
import hashlib
import requests
from bs4 import BeautifulSoup as bs
from tqdm import tqdm
from .crawler import Crawler
from .utils import clean_text, join_content
from ..utils import save, load



class PapersWithCode(Crawler):
    data_name = 'PapersWithCode'

    def __init__(self, database, website=None):
        super().__init__(self.data_name, database, website)
        self.num_datasets_per_query = website[self.data_name].get('num_datasets_per_query', 10)
        self.root_url = 'https://paperswithcode.com'
        self.datasets = self.make_datasets()
        self.num_datasets = len(self.datasets)

    def make_datasets(self):
        if self.use_cache and os.path.exists(os.path.join(self.cache_dir, 'datasets')):
            datasets = load(os.path.join(self.cache_dir, 'datasets'))
            return datasets

        datasets = set()
        # Pagination is simulated by appending page numbers
        for i in range(1, 2):
            URL = self.root_url + "/datasets/" + f'?page={i}'
    
            # 发出请求并解析页面
            response = requests.get(URL)
            response.encoding = 'utf-8'
            soup = bs(response.content, 'html.parser')
    
            # 抓取所有以 '/dataset' 开头的链接
            dataset_links = soup.select('a[href^="/dataset"]')
            #print(f"Page {page}: Found {len(dataset_links)} dataset links")  # Debug 打印

            for link in dataset_links:
                if link['href'].split("/")[-1] != "datasets":
                    datasets.add(link['href'])
                    #print(f"Dataset link: {link['href']}")  # 打印每个数据集链接

        # 暂存到缓存文件中
        datasets = sorted([i for i in datasets if i.split("/")[-1] != "datasets"], key=lambda x: x.split('/')[-1])
        save(datasets, os.path.join(self.cache_dir, 'datasets'))
        '''
        while True:
            response = requests.get(self.root_url + f"?page={page}")
            response.encoding = 'utf-8'
            soup = bs(response.content, 'html.parser')

            dataset_links = soup.select('a[href^="/dataset"]')
            if not dataset_links:
                break  # No more datasets to fetch

            for link in dataset_links:
                datasets.add(link['href'])

            if len(dataset_links) < self.num_datasets_per_query:
                break  # Break if fewer datasets than expected on the page

            page += 1

        datasets = sorted(list(datasets), key=lambda x: x.split('/')[-1])
        save(datasets, os.path.join(self.cache_dir, 'datasets'))
        '''

        return datasets

    def make_data(self, url, soup):
        index = hashlib.sha256(url.encode()).hexdigest()
        data = {}
        data['index'] = index
        data['URL'] = url

        # Parse datasets from soup
        elements = soup.find_all(['h1', 'p', 'a', 'footer', 'h4', 'h5'])
        current_group = {'header': None, 'content': []}

        cookie_keywords = ["cookie", "privacy", "consent", "policy"]

        if_first = True
        if_h4 = False
        for element in elements:
            if element.name == 'footer' or element.get('class') == ['footer'] or element.get('id') == 'footer':
                break
            if element.name == 'h1' and not if_h4:
                if current_group['header'] is not None:
                    if len(current_group['content']) > 0:
                            data[current_group['header'].strip().split("\n")[0]] = join_content(current_group['content'])
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
                            data[current_group['header'].strip().split("\n")[0]] = join_content(current_group['content'])
                header = element.get_text()
                current_group = {'header': header, 'content': []}
            elif element.name in ['p', 'a']:
                if "https" in element.get_text():
                    continue
                content = element.get_text().strip().replace("Add a new result", "").replace("Link an existing benchmark", "").replace("Add", "").replace("Remove", "")
                if any(keyword.lower() in content.lower() for keyword in cookie_keywords):
                    continue
                current_group['content'].append(content)

        if current_group['header'] is not None:
            if len(current_group['content']) > 0:
                if if_first:
                    data['Title'] = clean_text(current_group['header'])
                    data['Description'] = join_content(current_group['content'])
                else:
                    data[current_group['header'].strip().split("\n")[0]] = join_content(current_group['content'][:-4])
        return data

    def crawl(self):
        if not self.attempts_check():
            return
        datasets = self.datasets[:self.num_attempts] if self.num_attempts else self.datasets
        indices = range(len(datasets))
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
