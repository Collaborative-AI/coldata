import pymongo
from pymilvus import connections, utility, DataType, FieldSchema, CollectionSchema, Collection
from sentence_transformers import SentenceTransformer
import time
import yaml
import argparse

class VectorDatabase:
    def __init__(self, client_url, milvus_host="localhost", milvus_port="19530", model=SentenceTransformer('all-MiniLM-L6-v2')):
        self.client_url = client_url
        self.milvus_host = milvus_host
        self.milvus_port = milvus_port
        self.model = model
        self.embeddings = None
        self.collection = None

    def load_embeddings(self):
        client = pymongo.MongoClient(self.client_url)
        db_emb = client['Final_embedding']
        collec = db_emb['collection']
        self.embeddings = list(collec.find())

    def connect_to_docker(self):
        connections.connect("default", host=self.milvus_host, port=self.milvus_port)

    def create_vdb(self):
        somelist = self.embeddings
        # check and drop
        if "ColAI_search" in utility.list_collections():
            utility.drop_collection("ColAI_search")
        assert 'ColAI_search' not in utility.list_collections()

        fields = [
            FieldSchema(name="id", dtype=DataType.VARCHAR, max_length=500, is_primary=True, auto_id=False),
            FieldSchema(name="title", dtype=DataType.VARCHAR, max_length=500),
            FieldSchema(name="source", dtype=DataType.VARCHAR, max_length=500),
            FieldSchema(name="page_content", dtype=DataType.FLOAT_VECTOR, dim=384)
        ]
        collection_name = 'ColAI_search'
        schema = CollectionSchema(fields, "search datasets")
        self.collection = Collection(name=collection_name, schema=schema)

        entities = [
            [somelist[i]['id'] for i in range(len(somelist))],  # field id
            [somelist[i]['title'] for i in range(len(somelist))],  # field title
            [somelist[i]['source'] for i in range(len(somelist))],  # field source
            [somelist[i]['page_content'] for i in range(len(somelist))]
        ]
        insert_result = self.collection.insert(entities)

        index = {
            "index_type": "IVF_FLAT",
            "metric_type": "COSINE",
            "params": {"nlist": 1024},
        }
        self.collection.create_index(
            field_name="page_content", 
            index_params=index
        )

        time.sleep(5)
        

    def recover_vdb(self):
        assert 'ColAI_search' in utility.list_collections()
        self.collection = Collection(name='ColAI_search')

    def load_collection(self):
        self.collection.load()

    def search(self, query):
        search_params = {
            "metric_type": "COSINE",  
            "params": {"nprobe": 10}
        }

        results = self.collection.search(
            data=[self.model.encode([query], convert_to_tensor=True)[0].tolist()], 
            anns_field="page_content", 
            param=search_params,
            limit=2,
            expr=None,
            output_fields=['title', 'id', 'source'],
        )

        ids = results[0].ids
        print("Retrieved IDs:", ids)
        hit = results[0][0]
        print("Hit Title:", hit.entity.get('title'))

        return ids

    def release(self):
        self.collection.release()

# Example usage:
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Vector Database')
    parser.add_argument('--config', required=True, help='Path to the YAML config file')
    args = parser.parse_args()
    with open(args.config, 'r') as file:
        config = yaml.safe_load(file)
    client_url=config['database'].get('client_url', '')
    # init
    # print(client_url)
    vdb = VectorDatabase(client_url)
    vdb.load_embeddings()
    vdb.connect_to_docker()                                                                                                   
    
    # create vdb # or recover vdb
    vdb.create_vdb()
    #vdb.recover_vdb()

    vdb.load_collection()

    # search
    query = 'found this data helpful, a vote is appreciated'
    ids = vdb.search(query)

    # release
    vdb.release()
