from ucicrawler import UCI
UCIs = UCI()
UCIs.process_data()
UCIs.upload_data()
from kagglecrawler import Kaggle
Kaggle().process_data()
Kaggle().upload_data()
