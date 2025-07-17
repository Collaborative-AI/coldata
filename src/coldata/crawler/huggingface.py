import hashlib
import kaggle
import os
import pandas as pd
import time
from tqdm import tqdm
from .crawler import Crawler
from ..utils import save, load


class Kaggle(Crawler):
    data_name = 'Kaggle'

    def __init__(self, database, website=None, **kwargs):
        super().__init__(self.data_name, database, website, **kwargs)
        self.root_url = 'https://www.kaggle.com/datasets/'
        self.api = kaggle.KaggleApi()
        self.api.authenticate()
        self.datasets = self.make_datasets()
        self.num_datasets = len(self.datasets)

    def make_datasets(self):
        if self.num_attempts is not None and self.num_attempts == 0:
            return []

        cache_path = os.path.join(self.cache_dir, 'datasets')
        if self.use_cache and os.path.exists(cache_path):
            return load(cache_path)

        datasets = []
        page = 1
        page_size = 100  # max supported by API

        while True:
            result = self.api.datasets_list(page=page, page_size=page_size)
            if not result:
                break
            for ds in result:
                if ds.ref:
                    datasets.append(ds.ref)
            page += 1
            time.sleep(1)  # be polite to the API

        save(datasets, cache_path)
        return datasets

    def make_data(self, metadata):
        data = {
            'website': 'Kaggle',
            "id": metadata.id,
            "title": metadata.title,
            "subtitle": metadata.subtitle,
            "description": metadata.description,
            "owner": metadata.owner_user.username if metadata.owner_user else "",
            "datasetSlug": metadata.slug,
            "usabilityRating": metadata.usability_rating,
            "totalViews": metadata.total_views,
            "totalVotes": metadata.total_votes,
            "totalDownloads": metadata.total_downloads,
            "isPrivate": metadata.is_private,
            "keywords": metadata.keywords,
            "licenses": [license.name for license in metadata.licenses] if metadata.licenses else [],
            "index": hashlib.sha256((self.root_url + metadata.ref).encode()).hexdigest(),
            "URL": self.root_url + metadata.ref,
        }
        if data["description"]:
            data["description"] = (
                data["description"]
                .replace("![image](", "")
                .replace(")", "")
            )
        return data

    def crawl(self, is_upload=False):
        if not self.attempts_check():
            return

        indices = range(min(self.num_attempts, len(self.datasets))) if self.num_attempts else range(len(self.datasets))
        data = []

        for i in tqdm(indices):
            dataset_ref = self.datasets[i]
            url_i = self.root_url + dataset_ref
            index_i = hashlib.sha256(url_i.encode()).hexdigest()

            existing_data = self.database.collection.find_one({'index': index_i})
            if existing_data is None:
                try:
                    meta = self.api.dataset_view(dataset_ref)
                    meta.index = index_i
                    meta.URL = url_i
                    data_i = self.make_data(meta)
                    if is_upload:
                        self._upload_data(data_i, self.verbose)
                    else:
                        if self.query_interval > 0:
                            time.sleep(self.query_interval)
                    data.append(data_i)
                except Exception as e:
                    print(f"Error processing {dataset_ref}: {e}")
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