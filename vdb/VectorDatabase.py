import pymongo
from pymilvus import connections, utility, Collection
from sentence_transformers import SentenceTransformer

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

    def initiate_vdb(self):
        assert 'ColAI_search' not in utility.list_collections()
        self.collection = self.recover_vdb(self.embeddings)

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
    
    client_url = ''

    vdb = VectorDatabase(client_url)
    vdb.load_embeddings()
    vdb.connect_to_docker()
    
    # Initiate vdb # or recover vdb
    vdb.initiate_vdb()
    #vdb.recover_vdb()

    vdb.load_collection()

    # Search
    query = 'found this data helpful, a vote is appreciated'
    ids = vdb.search(query)

    vdb.release()
