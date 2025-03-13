import hashlib
import kaggle
import json
import os
import time
from tqdm import tqdm
from .crawler import Crawler
from ..utils import save, load


class Kaggle(Crawler):
    data_name = 'Kaggle'

    def __init__(self, database, website=None, **kwargs):
        super().__init__(self.data_name, database, website, **kwargs)
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
        while True:   # TODO: need to test termination
            try:
                result = self.api.dataset_list(page=self.page)
                if result is None:
                    print('No datasets found on page {}.'.format(self.page))
                    break
                datasets.extend(result)
                attempts_count += len(result)
                if self.num_attempts is not None and attempts_count >= self.num_attempts:
                    print('Reached the maximum number of attempts: {}'.format(self.num_attempts))
                    break
                self.page += 1
                time.sleep(self.query_interval)
            except Exception as e:
                print('Error fetching datasets on page {}: {}'.format(self.page, e))
                time.sleep(self.query_interval * 10)
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
            indices = range(min(self.num_attempts, len(list(self.datasets))))
        else:
            indices = range(len(list(self.datasets)))
        if os.path.exists(os.path.join(self.cache_dir, self.tmp_metadata_filename)):
            os.remove(os.path.join(self.cache_dir, self.tmp_metadata_filename))
        data = []
        for i in tqdm(indices):
            dataset = self.datasets[i]
            index = hashlib.sha256(dataset.url.encode()).hexdigest()
            self.api.dataset_metadata(dataset.ref, path=self.cache_dir)
            with open(os.path.join(self.cache_dir, self.tmp_metadata_filename), 'r') as file:
                data_i = json.load(file)
            data_i['index'] = index
            data_i['URL'] = dataset.url
            data_i = self.make_data(data_i)
            os.remove(os.path.join(self.cache_dir, self.tmp_metadata_filename))
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
