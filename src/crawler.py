from coldata.crawler import UCI

UCIs = UCI()
UCIs.process_data()
UCIs.upload_data()

from crawler.kaggle import Kaggle

Kaggle().process_data()
Kaggle().upload_data()
