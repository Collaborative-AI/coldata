import argparse
import yaml
import pymongo
from langchain.schema import Document
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.embeddings import HuggingFaceEmbeddings
from sentence_transformers import SentenceTransformer
from pymilvus import connections, utility, DataType, FieldSchema, CollectionSchema, Collection
import time


class VDB:
    def __init__(self, host, port, model_name, chunk_size=1024, chunk_overlap=256, add_start_index=True,
                 normalize_embeddings=False, device='cpu'):
        self.host = host
        self.port = port
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.add_start_index = add_start_index
        self.model_name = model_name
        self.normalize_embeddings = normalize_embeddings
        self.device = device
        self.text_splitter = RecursiveCharacterTextSplitter(chunk_size=self.chunk_size,
                                                            chunk_overlap=self.chunk_overlap,
                                                            add_start_index=self.add_start_index)

    def process(self, database):
        documents = self.make_documents(database)
        return

    def make_documents(self, database):
        collection = database.collection
        records = collection.find()
        documents = []
        for record in records:
            document = self.record_to_document(record)
            splitted_documents = self.text_splitter.split_documents([document])
            documents.extend(splitted_documents)
        return documents

    def record_to_document(self, record):
        metadata_keys = ['_id', 'index', 'URL']  # Define the fields that should be metadata
        metadata = {key: str(record[key]) for key in metadata_keys if key in record}
        # Combine the remaining key-value pairs into a single string for page content
        page_content = "\n".join([f"{key}: {value}" for key, value in record.items() if key not in metadata_keys])
        document = Document(page_content=page_content, metadata=metadata)
        return document

    def create_embed_model(self):
        print(type(self.model_kwargs))
        print(self.model_kwargs)
        print(type(self.encode_kwargs))
        print(self.encode_kwargs)
        embed_model = HuggingFaceEmbeddings(
            model_name=self.model_name,
            model_kwargs=self.model_kwargs,
            encode_kwargs=self.encode_kwargs
        )
        print("model created")
        self.model = SentenceTransformer('all-MiniLM-L6-v2')
        return embed_model

    def embed(self, model, splitted_text):
        for i in range(len(splitted_text)):
            splitted_text[i].page_content = model.embed_documents([splitted_text[i].page_content])[0]
        for i in range(len(splitted_text)):
            temp = splitted_text[i].metadata
            temp['page_content'] = splitted_text[i].page_content
            splitted_text[i] = temp
        part_list = []
        for i in range(len(splitted_text)):
            part_list.append(splitted_text[i])
        print("embedded")
        return part_list

    def connect_to_docker(self):
        connections.connect("default", host=self.milvus_host, port=self.milvus_port)

    def update_vdb(self, embeddings):

        somelist = embeddings
        # check and drop
        if "ColAI_search" not in utility.list_collections():
            fields = [
                FieldSchema(name="id", dtype=DataType.VARCHAR, max_length=500, is_primary=True, auto_id=False),
                FieldSchema(name="title", dtype=DataType.VARCHAR, max_length=500),
                FieldSchema(name="source", dtype=DataType.VARCHAR, max_length=500),
                FieldSchema(name="page_content", dtype=DataType.FLOAT_VECTOR, dim=384)
            ]
            collection_name = 'ColAI_search'
            schema = CollectionSchema(fields, "search datasets")
            self.milvus_collection = Collection(name=collection_name, schema=schema)
            index = {
                "index_type": "IVF_FLAT",
                "metric_type": "COSINE",
                "params": {"nlist": 1024},
            }
            self.milvus_collection.create_index(
                field_name="page_content",
                index_params=index
            )
            while not self.milvus_collection.has_index():
                print("Waiting for index creation to complete...")
                time.sleep(5)

        else:
            self.milvus_collection = Collection(name='ColAI_search')
        entities = [
            [somelist['id']],  # field id
            [somelist['title']],  # field title
            [somelist['source']],  # field source
            [somelist['page_content']]
        ]
        insert_result = self.milvus_collection.insert(entities)

    def recover_vdb(self):
        assert 'ColAI_search' in utility.list_collections()
        self.milvus_collection = Collection(name='ColAI_search')

    def load_collection(self):
        self.milvus_collection.load()

    def search(self, query):
        search_params = {
            "metric_type": "COSINE",
            "params": {"nprobe": 10}
        }

        results = self.milvus_collection.search(
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
        self.milvus_collection.release()


def main():
    parser = argparse.ArgumentParser(description='Data Processor')
    parser.add_argument('--config', required=True, help='Path to the YAML config file')
    args = parser.parse_args()

    # Parse the configuration from the YAML file
    with open(args.config, 'r') as file:
        config = yaml.safe_load(file)

    # Instantiate the DataProcessor class using the parsed configuration
    data_processor = DataProcessor(
        milvus_host=config['connection'].get('milvus_host', 'localhost'),
        milvus_port=config['connection'].get('milvus_port', '19530'),
        client_url=config['database'].get('client_url', ''),
        db_name=config['database'].get('db_name', 'Crawl-Data'),
        collection_name=config['database'].get('collection_name', 'metadata'),
        chunk_size=config['model'].get('chunk_size', 1024),
        chunk_overlap=config['model'].get('chunk_overlap', 0),
        add_start_index=config['model'].get('add_start_index', True),
        model_name=config['model'].get('model_name', 'sentence-transformers/all-MiniLM-L6-v2'),
        model_kwargs=config['model'].get('model_kwargs', {}),
        encode_kwargs=config['model'].get('encode_kwargs', '{"normalize_embeddings": false}'),
        query=config['search'].get('query', 'found this data helpf ul, a vote is appreciated'),
        k=config['search'].get('k', 10)
    )
    # Use the methods as needed
    client = pymongo.MongoClient(data_processor.client_url)
    files = data_processor.load_data(client)

    docs = []
    for file in files[:100]:
        docs.append(data_processor.convert_to_document(file))
    split_text = []
    for doc in docs:
        res = data_processor.split_texts([doc])
        split_text += res

    embed_model = data_processor.create_embed_model()
    embedded_data = data_processor.embed(embed_model, split_text)
    data_processor.connect_to_docker()
    for data in embedded_data:
        data_processor.update_vdb(data)
    # vdb.recover_vdb()

    data_processor.load_collection()

    # search
    query = 'found this data helpful, a vote is appreciated'
    ids = data_processor.search(query)

    # release
    data_processor.release()


if __name__ == '__main__':
    main()
