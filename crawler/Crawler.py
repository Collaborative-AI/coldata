import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
from tqdm import tqdm
import json
import os
from kaggle.api.kaggle_api_extended import KaggleApi
import pymongo
class Crawler:
    def __init__(self,save_path):
        self.save_path=save_path #存储路径
        self.url="" #每个URL不同
        self.dt={}
        client = pymongo.MongoClient("")
        db = client['Crawl-Data']
        self.collection = db['metadata']
        
    def process_data(self):
        pass
    
    def upload_data(self):
        pass
class UCI(Crawler):
    def __init__(self):
        self.url="https://archive.ics.uci.edu/ml/datasets.php"
        
    def process_data(self):
        record = self.collection.distinct("url")
        urls = set()
        URL = "https://archive.ics.uci.edu/datasets?skip=0&take=1000&sort=desc&orderBy=NumHits&search=" # assuming uci archive size no more than 1k
        page = requests.get(URL)
        soup = bs(page.content, "html.parser")
        for h2 in soup.find_all('h2'):
          # check if url already exists in mongodb
            if h2.find('a')['href'] not in record:
                urls.add(h2.find('a')['href'])
        df = pd.DataFrame(columns = ["id", "url"])
        page0 = requests.get("https://archive.ics.uci.edu" + list(urls)[0])
        soup0 = bs(page0.text, "html.parser")
        tb = pd.DataFrame(columns = ["Title","Description"] + [i.text for i in soup0.find_all('h1')][1:7])
        for url in tqdm(list(urls)):
            df.loc[len(df)] = [url[url.rfind("/")+1:], "https://archive.ics.uci.edu" + url]
            page0 = requests.get("https://archive.ics.uci.edu" + url)
            soup0 = bs(page0.text, "html.parser")
            txt2 = soup0.find_all(['h1','p'])
            tb.loc[len(tb)] = [soup0.find('h1').text] + [i.text for i in soup0.find_all('p')][:7]
        final_data = json.loads(pd.concat([df, tb], axis = 1).to_json(orient = "records"))
        # save to local instead of saving in memory to counter with potential interruptions
        with open('/content/metadata.json/UCI.json', 'w') as final_file:
            json.dump(final_data, final_file)
        #self.save
        
    def upload_data(self):
        # open file from local instead of accessing memory
        filename = '/content/metadata.json/UCI.json'
        with open(filename, 'r') as file:
              self.dt = json.load(file)
        count = 0
        for data in self.dt:
            # exists duplicate names, but different datasets, so url is used as key
            existing_data = self.collection.find_one({"url": data["url"]})
            if existing_data is None:
                # Data is not in the collection, so insert it
                self.collection.insert_one(data)
                count += 1
        print('Inserted', count, "number of data")
class Kaggle(Crawler):
    def __init__(self):
        self.count=0
    def process_data(self):
        api = KaggleApi()
          #os.environ['KAGGLE_CONFIG_DIR'] = '/content'
        api.authenticate()
          # Updated Version
          # Fetch the list of datasets, now we are fetching 10000, 20 per page
        record = self.collection.distinct("url")
        datasets = api.dataset_list(page=496)
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
                    with open('/content/metadata.json/kaggle'+str(self.count)+'.json', 'w') as final_file:
                          json.dump(metadata, final_file)
                    self.count += 1

    def upload_data(self):
        output_folder = '/content/metadata.json/'
        print(self.processcount)
        while True:
            filename = os.path.join(output_folder, f'kaggle{self.count}.json')
            try:
                with open(filename, 'r') as file:
                    metadata = json.load(file)
                    print(metadata['url'])
                    existing_data = self.collection.find_one({"url": metadata['url']})
                    if existing_data is None:
                        print('add')
                    # Data is not in the collection, so insert it
                        self.collection.insert_one(metadata)
                self.uploadcount += 1
            except FileNotFoundError:
                  break
        if self.count==self.uploadcount:
            print("fully uploaded")





