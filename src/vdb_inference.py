from vdb import set_up_preprocessor
import pymongo

def vdb_inference():
    data_processor = set_up_preprocessor()
    data_processor.load_collection()

    # search
    query = 'found this data helpful, a vote is appreciated'
    ids = data_processor.search(query)

    # release
    data_processor.release()
    return ids