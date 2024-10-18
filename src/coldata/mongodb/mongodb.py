import pymongo
from collections import defaultdict


class MongoDB:
    def __init__(self, mode, key, collection_name='dataset', index_field='index'):
        self.mode = mode
        self.key = key[mode]
        self.collection_name = collection_name
        try:
            self.client = pymongo.MongoClient(self.key['string'])
            self.db = self.client[self.key['db_name']]
            self.collection = self.db[self.collection_name]
            print(f"Connected to mongodb: {self.key['db_name']} ({self.collection_name})")
            self.create_index(index_field)
        except Exception as e:
            raise Exception(f"Failed to connect to MongoDB: {e}")

    def create_index(self, field):
        try:
            existing_indexes = self.collection.index_information()
            if field not in existing_indexes:
                self.collection.create_index([(field, pymongo.ASCENDING)], name=field)
                print(f"Index created for {field}")
        except Exception as e:
            raise Exception(f"Failed to create index: {e}")
        return

    def collection_structure(self, sample_size=100):
        schema = defaultdict(set)
        try:
            cursor = self.collection.find().limit(sample_size)
            for doc in cursor:
                for key, value in doc.items():
                    schema[key].add(type(value).__name__)
            print(f"Structure of 'dataset' collection:")
            for key, value_types in schema.items():
                print(f"{key}: {', '.join(value_types)}")
        except Exception as e:
            raise Exception(f"Error analyzing collection structure: {e}")
        return schema
