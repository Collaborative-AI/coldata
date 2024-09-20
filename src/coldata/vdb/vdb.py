import os
from langchain.schema import Document
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_huggingface import HuggingFaceEmbeddings
from pymilvus import connections, utility, DataType, FieldSchema, CollectionSchema, Collection


class VDB:
    def __init__(self, collection_name='dataset', alias='default', host='localhost', port='19530',
                 index_type='IVF_FLAT', metric_type='IP', nlist=1024, nprobe=1024, limit=4,
                 chunk_size=1024, chunk_overlap=256, add_start_index=True,
                 model_name='all-mpnet-base-v2', cache_folder='output', multi_process=False, show_progress=True,
                 device='cpu', normalize_embeddings=False):
        self.collection_name = collection_name
        self.alias = alias
        self.host = host
        self.port = port
        self.index_type = index_type
        self.metric_type = metric_type
        self.nlist = nlist
        self.nprobe = nprobe
        self.limit = limit

        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.add_start_index = add_start_index

        self.model_name = model_name
        self.cache_folder = cache_folder
        self.multi_process = multi_process
        self.show_progress = show_progress
        self.device = device
        self.normalize_embeddings = normalize_embeddings

        self.text_splitter = RecursiveCharacterTextSplitter(chunk_size=self.chunk_size,
                                                            chunk_overlap=self.chunk_overlap,
                                                            add_start_index=self.add_start_index)
        self.embedding_model = self.make_embedding_model()
        self.embedding_size = self.make_embedding_size()
        self.connect_to_milvus()
        self.collection = self.make_collection()
        self.load()

    def update(self, database):
        documents = self.make_documents(database)
        embeddings = self.make_embeddings_from_documents(documents)
        index = self.make_index(documents)
        self.insert(index, embeddings)
        self.flush()
        return

    ## TODO: batch queries
    def search(self, query):
        embedding = self.make_embedding_from_query(query)
        search_params = {
            "metric_type": self.metric_type,
            "params": {"nprobe": self.nprobe}
        }
        result = self.collection.search(
            data=[embedding],  # The query vector(s)
            anns_field="vector",  # The name of the vector field in the collection
            param=search_params,  # Search parameters
            limit=self.limit,  # Number of nearest neighbors to retrieve
            output_fields=["index"]  # Fields to return in the result (like IDs or other metadata)
        )
        return result

    def make_documents(self, database):
        collection = database.collection
        records = collection.find()
        documents = []
        for record in records:
            document = self.record_to_document(record)
            splitted_documents = self.text_splitter.split_documents([document])
            ## TODO: add splitted index
            documents.extend(splitted_documents)
        return documents

    def record_to_document(self, record):
        metadata_keys = ['_id', 'index', 'URL']  # Define the fields that should be metadata
        metadata = {key: str(record[key]) for key in metadata_keys if key in record}
        # Combine the remaining key-value pairs into a single string for page content
        page_content = "\n".join([f"{key}: {value}" for key, value in record.items() if key not in metadata_keys])
        document = Document(page_content=page_content, metadata=metadata)
        return document

    def make_embedding_model(self):
        cache_folder = os.path.join(self.cache_folder, self.model_name)
        model_kwargs = {'device': self.device}
        encode_kwargs = {'normalize_embeddings': self.normalize_embeddings}
        try:
            model_kwargs['local_files_only'] = True
            embedding_model = HuggingFaceEmbeddings(model_name=self.model_name, cache_folder=cache_folder,
                                                    multi_process=self.multi_process, show_progress=self.show_progress,
                                                    model_kwargs=model_kwargs, encode_kwargs=encode_kwargs)
        except Exception as e:
            model_kwargs['local_files_only'] = False
            embedding_model = HuggingFaceEmbeddings(model_name=self.model_name, cache_folder=cache_folder,
                                                    multi_process=self.multi_process, show_progress=self.show_progress,
                                                    model_kwargs=model_kwargs, encode_kwargs=encode_kwargs)
        return embedding_model

    def make_embedding_size(self):
        embedding_size = self.embedding_model.client.get_sentence_embedding_dimension()
        return embedding_size

    def make_embeddings_from_documents(self, documents):
        texts = [doc.page_content for doc in documents]
        embeddings = self.embedding_model.embed_documents(texts)
        return embeddings

    def make_embedding_from_query(self, query):
        embedding = self.embedding_model.embed_query(query)
        return embedding

    def make_index(self, documents):
        index = [document.metadata['index'] for document in documents]
        return index

    def connect_to_milvus(self):
        connections.connect(alias=self.alias, host=self.host, port=self.port)
        return

    def make_collection(self):
        if utility.has_collection(self.collection_name):
            collection = Collection(name=self.collection_name)
        else:
            index_field = FieldSchema(name="index", dtype=DataType.VARCHAR, max_length=64, is_primary=True,
                                      auto_id=False)
            vector_field = FieldSchema(name="vector", dtype=DataType.FLOAT_VECTOR, dim=self.embedding_size)
            fields = [index_field, vector_field]
            schema = CollectionSchema(fields=fields, description="Embedding collection")
            collection = Collection(name=self.collection_name, schema=schema)
            index_params = {"index_type": self.index_type, "metric_type": self.metric_type,
                            "params": {"nlist": self.nlist}}
            collection.create_index(field_name="vector", index_params=index_params)
        return collection

    def insert(self, index, embeddings):
        self.collection.insert([index, embeddings])
        return

    def load(self):
        self.collection.load()
        return

    def release(self):
        self.collection.release()

    def flush(self):
        self.collection.flush()
        return

    def drop(self):
        self.collection.drop()
        return
