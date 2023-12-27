from modelized_crawler.uci_crawler import UCI

UCIs = UCI()
UCIs.process_data()
UCIs.upload_data()

from modelized_crawler.kaggle_crawler import Kaggle

Kaggle().process_data()
Kaggle().upload_data()
