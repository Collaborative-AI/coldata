import os
from abc import abstractmethod


class Crawler:

    def __init__(self, data_name, database, website, **kwargs):
        self.data_name = data_name
        self.database = database
        self.num_attempts = website[self.data_name]['num_attempts']
        self.use_cache = website[self.data_name]['use_cache']
        self.cache_dir = os.path.join('output', 'cache', self.data_name)

    @abstractmethod
    def crawl(self):
        """
        Process the raw data that has been crawled.
        Subclasses must implement this method.
        """
        pass

    @abstractmethod
    def upload(self):
        """
        Upload the processed data to the MongoDB collection.
        Subclasses must implement this method.
        """
        pass

    def attempts_check(self):
        if self.num_attempts is not None or (isinstance(self.num_attempts, int) and self.num_attempts <= 0):
            return
