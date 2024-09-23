from abc import abstractmethod


class Crawler:
    def __init__(self, database, **kwargs):
        self.database = database

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
