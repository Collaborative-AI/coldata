import os
import time
from abc import abstractmethod
from tqdm import tqdm


class Crawler:

    def __init__(self, data_name, database, website, parse, **kwargs):
        self.data_name = data_name
        self.database = database
        self.num_attempts = website[self.data_name]['num_attempts']
        self.use_cache = website[self.data_name]['use_cache']
        self.query_interval = website[self.data_name]['query_interval']
        self.query_interval_scaler = website[self.data_name]['query_interval_scaler']
        self.verbose = website[self.data_name]['verbose']
        self.cache_dir = os.path.join('output', 'cache', self.data_name)
        self.parse = parse

    @abstractmethod
    def crawl(self, is_upload=False):
        """
        Process the raw data that has been crawled.
        Subclasses must implement this method.
        """
        pass

    @abstractmethod
    def upload(self, data):
        """
        Upload the processed data to the MongoDB collection.
        Subclasses must implement this method.
        """
        pass

    def attempts_check(self):
        return self.num_attempts is None or (isinstance(self.num_attempts, int) and self.num_attempts > 0)

    def _upload_data(self, data, verbose=True):
        existing_data = self.database.collection.find_one({'index': data['index']})
        is_insert = False
        if existing_data is None:
            self.database.collection.insert_one(data)
            is_insert = True
        if verbose:
            if is_insert:
                tqdm.write('Insert: {}'.format(data['URL']))
            else:
                tqdm.write('Exist and skip: {}'.format(data['URL']))
        return is_insert

    def _insert_data(self, data_i, is_upload):
        if is_upload:
            is_insert = self._upload_data(data_i, self.verbose)
        else:
            is_insert = False
        if is_insert and self.query_interval > 0:
            time.sleep(self.query_interval)
        return
