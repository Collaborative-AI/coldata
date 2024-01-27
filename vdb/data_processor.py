import argparse
import yaml
import pymongo
from langchain.schema import Document
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.embeddings import HuggingFaceEmbeddings
from langchain.vectorstores import Milvus
from sentence_transformers import SentenceTransformer, util

class DataProcessor:
    def __init__(self, milvus_host, milvus_port, client_url, db_name, collection_name, chunk_size, chunk_overlap, add_start_index, model_name, model_kwargs, encode_kwargs, query, k):
        self.milvus_host = milvus_host
        self.milvus_port = milvus_port
        self.client_url = client_url
        self.db_name = db_name
        self.collection_name = collection_name
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.add_start_index = add_start_index
        self.model_name = model_name
        self.model_kwargs = model_kwargs
        self.encode_kwargs = encode_kwargs
        self.query = query
        self.k = k

    def load_data(self, client):
        db = client[self.db_name]
        print(db.list_collection_names())
        collection = db[self.collection_name]
        print(collection.count_documents({}))
        files = list(collection.find())
        print(files[0].keys())
        print(files[0])
        print("data loaded")
        return files

    def convert_to_document(self, files):
        docs = []
        for file in files:
            content = ""
            try:
                content = file['descriptionNullable']  # #uci
            except:
                content = file['Description']  # #kaggle
            try:
                title = file['title']  # #uci
            except:
                content = file['Title']  # #kaggle
            docs.append(
                Document(
                    page_content=content,
                    metadata={"title": title, "source": file['url'], "id": str(file["_id"]), "chunk_id": ""}
                )
            )
        assert len(docs) == len(files)
        print(docs[0].page_content)
        print(docs[0].metadata)
        print("converted to document")
        return docs

    def split_texts(self, docs):
        text_splitter = RecursiveCharacterTextSplitter(chunk_size=self.chunk_size, chunk_overlap=self.chunk_overlap, add_start_index=self.add_start_index)
        split_text = text_splitter.split_documents(docs)
        for i, chunk in enumerate(split_text):
            chunk.metadata["chunk_id"] = str(i)
        print("text splitted")
        return split_text

    def create_embed_model(self):
        print(type(self.model_kwargs))
        print(self.model_kwargs)
        embed_model = HuggingFaceEmbeddings(
            model_name=self.model_name,
            model_kwargs=self.model_kwargs,
            encode_kwargs=self.encode_kwargs
        )
        print("model created")
        return embed_model

    def create_vector_store(self, split_text, embed_model):
        vdb = Milvus.from_documents(
            split_text,
            embedding=embed_model,
            connection_args={"host": self.milvus_host, "port": self.milvus_port}
        )
        print("vector store created")
        return vdb

    def embed(self, model, splitted_text):
        for i in range(len(splitted_text)):
            splitted_text[i].page_content = model.encode([splitted_text[i].page_content], convert_to_tensor=True)[0].tolist()
        for i in range(len(splitted_text)):
            temp = splitted_text[i].metadata
            temp['page_content'] = splitted_text[i].page_content
            splitted_text[i] = temp
        part_list = []
        for i in range(len(splitted_text)):
            part_list.append(splitted_text[i])
        return part_list

    def backup_embeddings(self, client, somelist):
        print(client.list_database_names())
        db_emb = client['embedding']
        collec = db_emb['collection']
        inserted_data = collec.insert_many(somelist)
        print(collec.count_documents({}))
        file_embeddings_saved = list(collec.find())
        return

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
        model_kwargs=config['model'].get('model_kwargs', '{"device": "cpu"}'),
        encode_kwargs=config['model'].get('encode_kwargs', '{"normalize_embeddings": false}'),
        query=config['search'].get('query', 'found this data helpful, a vote is appreciated'),
        k=config['search'].get('k', 10)
    )

    # Use the methods as needed
    client = pymongo.MongoClient(data_processor.client_url)
    files = data_processor.load_data(client)
    docs = data_processor.convert_to_document(files)
    split_text = data_processor.split_texts(docs)
    embed_model = data_processor.create_embed_model()
    vdb = data_processor.create_vector_store(split_text, embed_model)
    embedded_data = data_processor.embed(embed_model, split_text)
    data_processor.backup_embeddings(client, embedded_data)

if __name__ == '__main__':
    main()