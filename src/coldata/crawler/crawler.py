import pymongo
import os
from abc import abstractmethod
from collections import defaultdict

class Crawler:
    def __init__(self, mongodb_key_path, attempts=None):
        self.mongodb_key_path = mongodb_key_path
        self.attempts = attempts
        if not os.path.exists(self.mongodb_key_path):
            raise FileNotFoundError(f"MongoDB key file not found at {self.mongodb_key_path}")
        try:
            with open(self.mongodb_key_path, 'r') as file:
                self.mongodb_key = file.read().strip()
            client = pymongo.MongoClient(self.mongodb_key)
            db = client['Crawl-Data']
            self.collection = db['metadata']
        except Exception as e:
            raise ValueError(f"An error occurred while connecting to MongoDB: {e}")

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

    def collection_structure(self, sample_size=100):
        schema = defaultdict(set)
        try:
            cursor = self.collection.find().limit(sample_size)
            for doc in cursor:
                for key, value in doc.items():
                    schema[key].add(type(value).__name__)
            print("Collection Structure:")
            for key, value_types in schema.items():
                print(f"{key}: {', '.join(value_types)}")
        except Exception as e:
            raise Exception(f"Error analyzing collection structure: {e}")
        return
