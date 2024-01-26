import argparse

def parse_arguments():
    parser = argparse.ArgumentParser(description='Data Processor')
    
    # configuration variables (settings related to Milvus and MongoDB): in config.py or env.sh
    parser.add_argument('--milvus_host', default='localhost', help='Milvus host')
    parser.add_argument('--milvus_port', default='19530', help='Milvus port')
    parser.add_argument('--db_name', default='Crawl-Data', help='Database name')
    parser.add_argument('--collection_name', default='metadata', help='Collection name')

    # program variables: in command-line arguments and parser 
    parser.add_argument('--query', default='found this data helpful, a vote is appreciated', help='Query string')
    parser.add_argument('--k', type=int, choices=range(1, 11), default=10, help='Number of nearest neighbors (K)')
    #?
    parser.add_argument('--client_url', required=True, help='MongoDB client URL')

    # instance/object variables: in the constructor of a class
    parser.add_argument('--chunk_size', type=int, default=1024, help='Chunk size')
    parser.add_argument('--chunk_overlap', type=int, default=0, help='Chunk overlap')
    parser.add_argument('--add_start_index', type=bool, default=True, help='Add start index')
    parser.add_argument('--model_name', default='sentence-transformers/all-MiniLM-L6-v2', help='Model name')
    parser.add_argument('--model_kwargs', default='{"device": "cpu"}', help='Model kwargs')
    parser.add_argument('--encode_kwargs', default='{"normalize_embeddings": false}', help='Encode kwargs')

    args = parser.parse_args()
    return args
