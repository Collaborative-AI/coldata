import argparse
import pymongo
from langchain.schema import Document
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.embeddings import HuggingFaceEmbeddings
from langchain.vectorstores import Milvus
from sentence_transformers import SentenceTransformer, util

def parse_arguments():
    parser = argparse.ArgumentParser(description='Data Processor')
    parser.add_argument('--milvus_host', default='localhost', help='Milvus host')
    parser.add_argument('--milvus_port', default='19530', help='Milvus port')
    parser.add_argument('--client_url', required=True, help='MongoDB client URL')
    parser.add_argument('--db_name', default='Crawl-Data', help='Database name')
    parser.add_argument('--collection_name', default='metadata', help='Collection name')
    parser.add_argument('--chunk_size', type=int, default=1024, help='Chunk size')
    parser.add_argument('--chunk_overlap', type=int, default=0, help='Chunk overlap')
    parser.add_argument('--add_start_index', type=bool, default=True, help='Add start index')
    parser.add_argument('--model_name', default='sentence-transformers/all-MiniLM-L6-v2', help='Model name')
    parser.add_argument('--model_kwargs', default='{"device": "cpu"}', help='Model kwargs')
    parser.add_argument('--encode_kwargs', default='{"normalize_embeddings": false}', help='Encode kwargs')
    parser.add_argument('--query', default='found this data helpful, a vote is appreciated', help='Query string')
    parser.add_argument('--k', type=int, choices=range(1, 11), default=10, help='Number of nearest neighbors (K)')

    args = parser.parse_args()
    return args

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
    args = parse_arguments()

    # Instantiate the DataProcessor class
    data_processor = DataProcessor(
        milvus_host=args.milvus_host,
        milvus_port=args.milvus_port,
        client_url=args.client_url,
        db_name=args.db_name,
        collection_name=args.collection_name,
        chunk_size=args.chunk_size,
        chunk_overlap=args.chunk_overlap,
        add_start_index=args.add_start_index,
        model_name=args.model_name,
        model_kwargs=args.model_kwargs,
        encode_kwargs=args.encode_kwargs,
        query=args.query,
        k=args.k
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