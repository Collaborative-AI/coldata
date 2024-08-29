import hashlib
import kaggle
import json
import os
from .crawler import Crawler


class Kaggle(Crawler):
    data_name = 'Kaggle'

    def __init__(self, key, num_attempts=None, num_datasets_per_query=20):
        super().__init__(self.data_name, key, num_attempts)
        self.num_datasets_per_query = num_datasets_per_query
        self.json_path = os.path.join('output', 'json')
        self.tmp_metadata_filename = 'dataset-metadata.json'

    def crawl(self):
        attempts_count = 0
        api = kaggle.KaggleApi()
        api.authenticate()
        datasets = api.dataset_list(page=self.num_datasets_per_query)
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
        count = 0
        print(f'Start uploading ({self.data_name})...')
        json_file_names = os.listdir(self.json_path)
        for json_file_name in json_file_names:
            with open(os.path.join(self.json_path, json_file_name), 'r') as file:
                data = json.load(file)
                existing_data = self.collection.find_one({'index': data['index']})
                if existing_data is None:
                    self.collection.insert_one(data)
                    count += 1
        print(f'Insert {count} records')
        return
