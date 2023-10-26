def kaggle_updates():
    import threading
    import time
    import subprocess
    from kaggle.api.kaggle_api_extended import KaggleApi
    import pymongo
    import json
    api = KaggleApi()
    count=0
  #os.environ['KAGGLE_CONFIG_DIR'] = '/content'
    api.authenticate()
    client = pymongo.MongoClient("")
    db = client['Crawl-Data']
    collection = db['metadata']
    record = collection.distinct("url")
    datasets = api.dataset_list()
    for dataset in datasets:
        print(dataset.url in record)
      #need to download the metadatafile and read it each time to get information
        if dataset.url not in record:
            try:
                metadata = api.dataset_metadata(dataset.ref, path = '/content/metadata.json')
            except:
                continue
            with open('/content/metadata.json/dataset-metadata.json', 'r') as metadata_file:
                metadata = json.load(metadata_file)
                metadata['name'] = dataset.ref
                metadata['url'] = dataset.url
            with open('/content/metadata.json/kaggle'+str(count)+'.json', 'w') as final_file:
                json.dump(metadata, final_file)
        else:
            break

    time.sleep(24 * 60 * 60)




