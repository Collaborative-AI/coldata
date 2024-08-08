import argparse
import yaml
import pymongo
import coldata


def main():
    parser = argparse.ArgumentParser(description='Data Processor')
    parser.add_argument('--config', required=True, help='Path to the YAML config file')
    args = parser.parse_args()

    # Parse the configuration from the YAML file
    with open(args.config, 'r') as file:
        config = yaml.safe_load(file)

    # ingest UCI
    UCIs = coldata.crawler.UCI()
    UCIs.process_data()
    UCIs.upload_data()

    # ingest kaggle
    coldata.crawler.Kaggle().process_data()
    coldata.crawler.Kaggle().upload_data()

    # vdb updates
    # Instantiate the DataProcessor class using the parsed configuration
    data_processor = coldata.vdb.DataProcessor(
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
    # testing for search
    '''
    data_processor.load_collection()

    # search
    query = 'found this data helpful, a vote is appreciated'
    ids = data_processor.search(query)
    
    # release
    data_processor.release()
    '''


if __name__ == '__main__':
    main()
