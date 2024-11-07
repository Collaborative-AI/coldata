from abc import abstractmethod


class Crawler:
    def __init__(self, database, **kwargs):
        self.database = database
        self.num_attempts = 0

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
