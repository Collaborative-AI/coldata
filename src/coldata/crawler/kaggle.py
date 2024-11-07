import hashlib
import kaggle
import json
import os
from .crawler import Crawler


class Kaggle(Crawler):
    data_name = 'Kaggle'

    def __init__(self, database, website=None):
        super().__init__(database)
        self.num_attempts = website[self.data_name]['num_attempts']
        self.init_page = website[self.data_name]['init_page']
        self.json_path = os.path.join('output', 'json')
        self.tmp_metadata_filename = 'dataset-metadata.json'

    def crawl(self):
        self.attempts_check()
        attempts_count = 0
        api = kaggle.KaggleApi()
        api.authenticate()

        self.page = self.init_page
        datasets = []
        while True:
            result = api.dataset_list(page=self.page)
            if not result or (self.num_attempts is not None and attempts_count >= self.num_attempts):
                break
            datasets.extend(result)
            attempts_count += len(result)
            self.page += 1

        attempts_count = 0
        for dataset in datasets:
            if self.num_attempts is not None and attempts_count < self.num_attempts:
                try:
                    api.dataset_metadata(dataset.ref, path=self.json_path)
                except:
                    continue
                index = hashlib.sha256(dataset.url.encode()).hexdigest()
                filename_template_i = '{}.json'.format(index)
                data_path = os.path.join(self.json_path, filename_template_i)
                if not os.path.exists(data_path):
                    with open(os.path.join(self.json_path, self.tmp_metadata_filename), 'r') as file:
                        data = json.load(file)
                        data['index'] = index
                        data['ref'] = dataset.ref
                        data['URL'] = dataset.url
                    with open(data_path, 'w') as file:
                        json.dump(data, file)
                    attempts_count += 1
                    os.remove(os.path.join(self.json_path, self.tmp_metadata_filename))
        return

    def upload(self):
        self.attempts_check()
        count = 0
        print(f'Start uploading ({self.data_name})...')
        json_file_names = os.listdir(self.json_path)
        for json_file_name in json_file_names:
            with open(os.path.join(self.json_path, json_file_name), 'r') as file:
                data = json.load(file)
                existing_data = self.database.collection.find_one({'index': data['index']})
                if existing_data is None:
                    self.database.collection.insert_one(data)
                    count += 1
        print(f'Insert {count} records')
        return
