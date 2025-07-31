import hashlib
import os
import requests
import time
import trafilatura
from bs4 import BeautifulSoup as bs
from tqdm import tqdm
from huggingface_hub import HfApi, list_datasets, dataset_info
from .crawler import Crawler
from ..utils import save, load


class HuggingFace(Crawler):
    data_name = 'HuggingFace'

    def __init__(self, database, website=None, **kwargs):
        super().__init__(self.data_name, database, website, **kwargs)
        self.root_url = 'https://huggingface.co/datasets/'
        self.api = HfApi()
        self.datasets = self.make_datasets()
        self.num_datasets = len(self.datasets)

    def make_datasets(self):
        if self.num_attempts is not None and self.num_attempts == 0:
            return []

        cache_path = os.path.join(self.cache_dir, 'datasets')
        if self.use_cache and os.path.exists(cache_path):
            return load(cache_path)

        datasets = []
        attempts_count = 0
        for ds in list_datasets():
            datasets.append(ds.id)
            attempts_count += 1
            if self.num_attempts is not None and attempts_count >= self.num_attempts:
                print('Reached the maximum number of attempts: {}'.format(self.num_attempts))
                break
        save(datasets, cache_path)
        return datasets

    def make_data(self, metadata, url, soup):
        index = hashlib.sha256(url.encode()).hexdigest()

        # Extract and clean HTML
        html_text = trafilatura.extract(str(soup), output_format=self.parse['output_format'])

        # Convert structured metadata into string format
        structured_info = {
            "ID": metadata.id,
            "Author": metadata.author,
            "Private": metadata.private,
            "Gated": metadata.gated,
            "Disabled": metadata.disabled,
            "Downloads": metadata.downloads,
            "Likes": metadata.likes,
            "Tags": ", ".join(metadata.tags) if metadata.tags else "",
            "SHA": metadata.sha,
            "Created at": metadata.created_at.isoformat(),
            "Last modified": metadata.last_modified.isoformat(),
            "License": metadata.cardData.get("license") if metadata.cardData else "",
            "Pretty name": metadata.cardData.get("pretty_name") if metadata.cardData else "",
            "Description (card)": metadata.cardData.get("description") if metadata.cardData else "",
        }

        # Format structured info as a readable string block
        structured_text = "\n".join(f"{k}: {v}" for k, v in structured_info.items() if v)

        # Combine structured and unstructured parts into one string
        combined_info = structured_text + "\n\n" + (html_text or "")
        info = combined_info.strip()
        # Final data record
        data = {"website": "HuggingFace", "index": index, "URL": url, "info": info}
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
            dataset_id = self.datasets[i]
            url_i = self.root_url + dataset_id
            index_i = hashlib.sha256(url_i.encode()).hexdigest()
            existing_data = self.database.collection.find_one({'index': index_i})
            if existing_data is None:
                try:
                    metadata = dataset_info(dataset_id)
                    page_i = requests.get(url_i)
                    soup_i = bs(page_i.text, 'html.parser')
                    main_card = soup_i.find('div', class_='prose')
                    data_i = self.make_data(metadata, url_i, main_card)
                    if is_upload:
                        self._upload_data(data_i, self.verbose)
                    else:
                        if self.query_interval > 0:
                            time.sleep(self.query_interval)
                    data.append(data_i)
                except Exception as e:
                    print(f"Error processing {dataset_id}: {e}")
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
