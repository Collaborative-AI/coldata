from modelized_crawler.uci import UCI

UCIs = UCI()
UCIs.process_data()
UCIs.upload_data()

from modelized_crawler.kaggle import Kaggle

Kaggle().process_data()
Kaggle().upload_data()
