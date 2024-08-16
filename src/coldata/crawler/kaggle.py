import time
import kaggle
import json
import os
from .crawler import Crawler


class Kaggle(Crawler):
    def __init__(self, mongodb_key_path, attempts=None):
        super().__init__(mongodb_key_path, attempts)
        self.processcount = 0
        self.uploadcount = 0

    def process_data(self):
        api = kaggle.KaggleApi()
        # os.environ['KAGGLE_CONFIG_DIR'] = '/content'
        api.authenticate()
        # Updated Version
        # Fetch the list of datasets, now we are fetching 10000, 20 per page
        record = self.collection.distinct("url")
        datasets = api.dataset_list(page=20)
        for dataset in datasets:
            # print(dataset.url in record)
            # need to download the metadatafile and read it each time to get information
            if dataset.url not in record:
                try:
                    metadata = api.dataset_metadata(dataset.ref, path=os.path.join('/content', 'metadata.json'))
                except:
                    continue
                directory = '/content/metadata.json'
                filename = 'dataset-metadata.json'
                file_path = os.path.join(directory, filename)
                with open(file_path, 'r') as metadata_file:
                    metadata = json.load(metadata_file)
                    metadata['name'] = dataset.ref
                    metadata['url'] = dataset.url
                directory = '/content/metadata.json'
                filename_template = 'kaggle{}.json'.format(self.processcount)
                filename = os.path.join(directory, filename_template)
                with open(filename, 'w') as final_file:
                    json.dump(metadata, final_file)
                self.processcount += 1

    def upload_data(self):
        output_folder = '/content/metadata.json/'
        print(self.processcount)
        while True:
            filename = os.path.join(output_folder, f'kaggle{self.processcount}.json')
            try:
                with open(filename, 'r') as file:
                    metadata = json.load(file)
                    print(metadata['url'])
                    existing_data = self.collection.find_one({"url": metadata['url']})
                    if existing_data is None:
                        print('add')
                        # Data is not in the collection, so insert it
                        self.collection.insert_one(metadata)
                self.uploadcount += 1
            except FileNotFoundError:
                break
        if self.processcount == self.uploadcount:
            print("fully uploaded")

    def kaggle_updates(self):
        api = kaggle.KaggleApi()
        count = 0
        # os.environ['KAGGLE_CONFIG_DIR'] = '/content'
        api.authenticate()
        record = self.collection.distinct("url")
        datasets = api.dataset_list(page=1000, sort_by="updated")
        print("start")
        for dataset in datasets:
            print(dataset.url in record)
            # need to download the metadatafile and read it each time to get information
            if dataset.url not in record:
                try:
                    metadata = api.dataset_metadata(dataset.ref, path=os.path.join('/content', 'metadata.json'))
                except:
                    continue
                directory = '/content/metadata.json'
                filename = 'dataset-metadata.json'
                file_path = os.path.join(directory, filename)
                with open(file_path, 'r') as metadata_file:
                    metadata = json.load(metadata_file)
                    metadata['name'] = dataset.ref
                    metadata['url'] = dataset.url
                directory = '/content/metadata.json'
                filename_template = 'kaggle{}.json'.format(self.processcount)
                filename = os.path.join(directory, filename_template)
                with open(filename, 'w') as final_file:
                    json.dump(metadata, final_file)
            else:
                break
        print("end")
        output_folder = '/content/metadata.json/'
        print(self.processcount)
        while True:
            filename = os.path.join(output_folder, f'kaggle{self.processcount}.json')
            try:
                with open(filename, 'r') as file:
                    metadata = json.load(file)
                    print(metadata['url'])
                    existing_data = self.collection.find_one({"url": metadata['url']})
                    if existing_data is None:
                        print('add')
                        # Data is not in the collection, so insert it
                        self.collection.insert_one(metadata)
                self.uploadcount += 1
            except FileNotFoundError:
                break
        if self.processcount == self.uploadcount:
            print("fully uploaded")
        time.sleep(60 * 60 * 24)
