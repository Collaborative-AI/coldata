import hashlib
import kaggle
import json
import os
import time
from tqdm import tqdm
from .crawler import Crawler
from ..utils import save, load, makedir_exist_ok


class Kaggle(Crawler):
    data_name = 'Kaggle'

    def __init__(self, database, website=None):
        super().__init__(self.data_name, database, website)
        self.init_page = website[self.data_name]['init_page']
        self.tmp_metadata_filename = 'dataset-metadata.json'
        self.api = kaggle.KaggleApi()
        self.api.authenticate()
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
            result = self.api.dataset_list(page=self.page)
            if not result or (self.num_attempts is not None and attempts_count >= self.num_attempts):
                break
            datasets.extend(result)
            attempts_count += len(result)
            self.page += 1

        save(datasets, os.path.join(self.cache_dir, 'datasets'))
        return datasets

    def make_data(self, metadata):
        data = {
            'website': 'Kaggle',
            "id": metadata.get("id", ""),
            "title": metadata.get("title", ""),
            "subtitle": metadata.get("subtitle", ""),
            "description": metadata.get("description", ""),
            "owner": metadata.get("ownerUser", ""),
            "datasetSlug": metadata.get("datasetSlug", ""),
            "usabilityRating": metadata.get("usabilityRating", ""),
            "totalViews": metadata.get("totalViews", 0),
            "totalVotes": metadata.get("totalVotes", 0),
            "totalDownloads": metadata.get("totalDownloads", 0),
            "isPrivate": metadata.get("isPrivate", False),
            "keywords": metadata.get("keywords", []),
            "licenses": [license_info.get("name", "") for license_info in metadata.get("licenses", [])],
            "index": metadata.get("index", ""),
            "URL": metadata.get("URL", ""),
        }

        # Clean up the description (e.g., remove markdown links)
        data["description"] = (
            data["description"]
            .replace("![image](", "")
            .replace(")", "")
            if data["description"]
            else ""
        )
        return data

    def crawl(self, is_upload=False):
        if not self.attempts_check():
            return

        if self.num_attempts is not None:
            datasets = self.datasets[:self.num_attempts]
        else:
            datasets = self.datasets
        if os.path.exists(os.path.join(self.cache_dir, self.tmp_metadata_filename)):
            os.remove(os.path.join(self.cache_dir, self.tmp_metadata_filename))
        data = []
        for dataset in datasets:
            index = hashlib.sha256(dataset.url.encode()).hexdigest()
            makedir_exist_ok(os.path.join(self.cache_dir, 'json'))
            data_path = os.path.join(self.cache_dir, 'json', '{}.json'.format(index))
            if not (self.use_cache and os.path.exists(data_path)):
                self.api.dataset_metadata(dataset.ref, path=self.cache_dir)
                with open(os.path.join(self.cache_dir, self.tmp_metadata_filename), 'r') as file:
                    data_i = json.load(file)
                    data_i['index'] = index
                    data_i['URL'] = dataset.url
                data_i = self.make_data(data_i)
                with open(data_path, 'w') as file:
                    json.dump(data_i, file)
                os.remove(os.path.join(self.cache_dir, self.tmp_metadata_filename))
                if is_upload:
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
        print(f'Start uploading ({self.data_name})...')
        filenames = os.listdir(os.path.join(self.cache_dir, 'json'))
        for i in tqdm(range(len(filenames))):
            with open(os.path.join(self.cache_dir, 'json', filenames[i]), 'r') as file:
                data_i = json.load(file)
                is_insert = self._upload_data(data_i, self.verbose)
                if is_insert:
                    count += 1
        print(f'Insert {count} records')
        return
