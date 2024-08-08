import pymongo


class Crawler:
    def __init__(self):
        self.url = ""  # 每个URL不同
        self.dt = {}
        self.url = ""

        client = pymongo.MongoClient(open('mongodb.txt', 'r').read())
        db = client['Crawl-Data']
        self.collection = db['metadata']

    def process_data(self):
        pass

    def upload_data(self):
        pass
