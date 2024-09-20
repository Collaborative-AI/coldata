import pymongo
from abc import abstractmethod
from collections import defaultdict

## TODO: merge datasets into one collection
class Crawler:
    def __init__(self, data_name, key, num_attempts, **kwargs):
        self.key = key
        self.num_attempts = num_attempts
        self.client = pymongo.MongoClient(self.key['string'])
        self.collection = self.client[self.key['db_name']][data_name]
        self.collection.create_index([('index', pymongo.ASCENDING)], name='index')

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
