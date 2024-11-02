import json
import hashlib
import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
from tqdm import tqdm


class AWS():
    data_name = 'AWS'

    def __init__(self, num_attempts=None, website=None):
        self.num_attempts = num_attempts
        self.root_url = 'https://registry.opendata.aws'

    def make_url(self):
        url = set()
        page = requests.get(self.root_url)
        soup = bs(page.content, 'html.parser')
        datasets = soup.find_all('div', class_='dataset')
        for dataset in tqdm(datasets):
            url.add(dataset.find('a')['href'])
        url = list(url)
        return url

    def make_table(self, url):
        page = requests.get(self.root_url + url[0])
        soup = bs(page.text, 'html.parser')
        table = pd.DataFrame(columns=['index'] + ['Title', 'Description', 'label'] +
                                     [i.text for i in soup.find_all('h4')][:7] + ['URL'])
        return table

    def crawl(self):
        url = self.make_url()
        table = self.make_table(url)
        if self.num_attempts is not None:
            indices = range(self.num_attempts)
        else:
            indices = range(len(list(url)))
        print(f'Start crawling ({self.data_name})...')
        for i in tqdm(indices):
            url_i = self.root_url + url[i]
            page_i = requests.get(url_i)
            soup_i = bs(page_i.text, 'html.parser')
            index_i = hashlib.sha256(url_i.encode()).hexdigest()
            table.loc[len(table)] = [index_i] + [soup_i.find('h1').text] + \
                                    [soup_i.find('div', class_='col-md-6').find('p').text] + \
                                    [''.join([i.text for i in soup_i.find_all('span', class_='label-info')])] + \
                                    [i.text for i in soup_i.find('div', class_='col-md-6').find_all('p')][1:4] + \
                                    [i.text for i in soup_i.find('div', class_='col-md-6').find_all('p')][-3:] + \
                                    [dict(zip([i.text for i in soup_i.find_all('h5')][:2], [i.text for i in soup_i.find_all('ul', class_ = "dataatwork-list")][:2]))] + \
                                    [url_i]
        print(table.head())
        data = table.to_json(orient='records')
        self.data = json.loads(data)
        return self.data

    def upload(self):
        count = 0
        print(f'Start uploading ({self.data_name})...')
        for data in tqdm(self.data):
            existing_data = self.database.collection.find_one({'index': data['index']})
            if existing_data is None:
                self.database.collection.insert_one(data)
                count += 1
        print(f'Insert {count} records.')
        return
