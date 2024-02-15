import requests
import numpy
import pandas as pd
from bs4 import BeautifulSoup as bs
from tqdm import tqdm
import json
import os
import pymongo


class Crawler:
    def __init__(self):
        self.url = ""  # 每个URL不同
        self.dt = {}
        self.url = ""

        client = pymongo.MongoClient(open('mongodb.txt','r').read())
        db = client['Crawl-Data']
        self.collection = db['metadata']

    def process_data(self):
        pass

    def upload_data(self):
        pass
