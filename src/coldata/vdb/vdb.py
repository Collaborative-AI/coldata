from langchain.schema import Document
from langchain.text_splitter import RecursiveCharacterTextSplitter
from pymilvus import connections, utility, DataType, FieldSchema, CollectionSchema, Collection
from collections import OrderedDict
from tqdm import tqdm
from .embed import Embedding


class VDB:
    def __init__(self, collection_name='dataset', alias='default', host='localhost', port='19530',
                 index_type='IVF_FLAT', metric_type='IP', nlist=1024, nprobe=1024, limit=4, renew=True,
                 page_limit=100, batch_size=128, chunk_size=1024, chunk_overlap=256, add_start_index=True,
                 model_name='all-mpnet-base-v2', snapshot_folder='output/snapshot', device='cpu', max_length=512,
                 normalize_embeddings=False):
        self.collection_name = collection_name
        self.alias = alias
        self.host = host
        self.port = port
        self.index_type = index_type
        self.metric_type = metric_type
        self.similarity_order = self.make_similarity_order()
        self.nlist = nlist
        self.nprobe = nprobe
        self.limit = limit
        self.renew = renew
        self.page_limit = page_limit
        self.batch_size = batch_size

        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.add_start_index = add_start_index

        self.model_name = model_name
        self.snapshot_folder = snapshot_folder
        self.device = device
        self.max_length = max_length
        self.normalize_embeddings = normalize_embeddings

        self.text_splitter = RecursiveCharacterTextSplitter(chunk_size=self.chunk_size,
                                                            chunk_overlap=self.chunk_overlap,
                                                            add_start_index=self.add_start_index)
        self.embedding_model = self.make_embedding_model()
        self.embedding_size = self.make_embedding_size()
        self.connect_to_milvus()
        self.collection = self.make_collection()
        self.load()

    def _embed_and_insert(self, texts, indices):
        # texts = [doc.page_content for doc in docs]
        embeddings = self.embedding_model.embed_documents(texts)
        if embeddings is not None:
            self.insert(indices, embeddings)
        return

    def update(self, database):
        collection = database.collection
        total_records = collection.count_documents({})
        cursor = collection.find()

        buffer_texts = []
        buffer_indices = []

        with tqdm(total=total_records, disable=not self.show_progress, desc="Updating Milvus") as pbar:
            for record in cursor:
                document = self.record_to_document(record)
                splitted_documents = self.text_splitter.split_documents([document])
                for i, splitted_document in enumerate(splitted_documents):
                    splitted_index = f"{splitted_document.metadata['index']}_{i}"
                    splitted_document.metadata['index'] = splitted_index
                    splitted_text = splitted_document.page_content
                    buffer_texts.append(splitted_text)
                    buffer_indices.append(splitted_index)

                if len(buffer_texts) >= self.batch_size:
                    self._embed_and_insert(buffer_texts, buffer_indices)
                    buffer_texts.clear()
                    buffer_indices.clear()
                pbar.update(1)

            if buffer_texts:
                self._embed_and_insert(buffer_texts, buffer_indices)

        self.flush()
        return

    def search(self, database, queries):
        embeddings = self.make_embedding_from_queries(queries)
        search_params = {
            "metric_type": self.metric_type,
            "params": {"nprobe": self.nprobe}
        }
        milvus_result = self.collection.search(
            data=embeddings,  # The query vector(s)
            anns_field="vector",  # The name of the vector field in the collection
            param=search_params,  # Search parameters
            limit=self.limit,  # Number of nearest neighbors to retrieve
            output_fields=["index"]  # Fields to return in the result (like IDs or other metadata)
        )
        result = []
        for milvus_result_i in milvus_result:
            result_i = {}
            for hit in milvus_result_i:
                mongodb_index = self.make_mongodb_index(hit.id)
                if mongodb_index in result_i:
                    if self.check_similarity_order(hit.distance, result_i[mongodb_index]['distance']):
                        result_i[mongodb_index]['distance'] = hit.distance
                else:
                    result_i[mongodb_index] = {'distance': hit.distance}
            result_i = OrderedDict(sorted(result_i.items(), key=lambda item: item[1]['distance'],
                                          reverse=self.similarity_order == 'greater'))
            indices_i = list(result_i.keys())
            mongodb_result_i = database.collection.find({"index": {"$in": indices_i}})

            for j, record in enumerate(mongodb_result_i):
                index_key = record['index']
                if index_key in result_i:
                    result_i[index_key]['record'] = record

            result.append(result_i)
        return result

    def make_documents(self, database):
        collection = database.collection
        records = collection.find()
        documents = []
        indices = []
        for record in records:
            document = self.record_to_document(record)
            splitted_documents = self.text_splitter.split_documents([document])
            for i, splitted_document in enumerate(splitted_documents):
                splitted_index = f"{splitted_document.metadata['index']}_{i}"
                splitted_document.metadata['index'] = splitted_index
                documents.append(splitted_document)
                indices.append(splitted_index)
        return documents, indices

    def record_to_document(self, record):
        metadata_keys = ['_id', 'index', 'URL']  # Define the fields that should be metadata
        metadata = {key: str(record[key]) for key in metadata_keys if key in record}
        # Combine the remaining key-value pairs into a single string for page content
        page_content = "\n".join([f"{key}: {value}" for key, value in record.items() if key not in metadata_keys])
        document = Document(page_content=page_content, metadata=metadata)
        return document

    def make_embedding_model(self):
        # TODO: needs debug test
        embedding_model = Embedding(model_name=self.model_name,
                                    snapshot_folder=self.snapshot_folder,
                                    device=self.device,
                                    max_length=self.max_length,
                                    normalize_embeddings=self.normalize_embeddings)

        # cache_folder = os.path.join(self.cache_folder, self.model_name)
        # model_kwargs = {'device': self.device}
        # encode_kwargs = {'normalize_embeddings': self.normalize_embeddings}
        # try:
        #     model_kwargs['local_files_only'] = True
        #     embedding_model = HuggingFaceEmbeddings(model_name=self.model_name, cache_folder=cache_folder,
        #                                             multi_process=self.multi_process, show_progress=self.show_progress,
        #                                             model_kwargs=model_kwargs, encode_kwargs=encode_kwargs)
        # except Exception as e:
        #     model_kwargs['local_files_only'] = False
        #     embedding_model = HuggingFaceEmbeddings(model_name=self.model_name, cache_folder=cache_folder,
        #                                             multi_process=self.multi_process, show_progress=self.show_progress,
        #                                             model_kwargs=model_kwargs, encode_kwargs=encode_kwargs)
        return embedding_model

    def make_similarity_order(self):
        if self.metric_type in ['IP', 'COSINE']:
            order = 'greater'
        else:
            order = 'smaller'
        return order

    def check_similarity_order(self, similarity_0, similarity_1):
        if self.similarity_order == 'greater':
            return similarity_0 > similarity_1
        else:
            return similarity_0 <= similarity_1

    def make_embedding_size(self):
        embedding_size = self.embedding_model.client.get_sentence_embedding_dimension()
        return embedding_size

    def make_embeddings_from_documents(self, documents):
        texts = [doc.page_content for doc in documents]
        if len(texts) > 0:
            embeddings = self.embedding_model.embed_documents(texts)
        else:
            embeddings = None
        return embeddings

    def make_embedding_from_queries(self, queries):
        embedding = self.embedding_model.embed_documents(queries)
        return embedding

    def make_index(self, documents):
        index = [document.metadata['index'] for document in documents]
        return index

    def make_mongodb_index(self, index):
        mongodb_index = index.split('_')[0]
        return mongodb_index

    def connect_to_milvus(self):
        connections.connect(alias=self.alias, host=self.host, port=self.port)
        return

    def make_collection(self):
        if utility.has_collection(self.collection_name):
            collection = Collection(name=self.collection_name)
            if self.renew:
                collection.drop()
                collection = self.make_collection()
        else:
            index_field = FieldSchema(name="index", dtype=DataType.VARCHAR, max_length=128, is_primary=True,
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

    def retrieve(self, epr=''):
        results = self.collection.query(
            expr=epr,
            limit=self.page_limit
        )
        return results

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
