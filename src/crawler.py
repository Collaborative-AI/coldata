import coldata

UCIs = coldata.crawler.UCI()
UCIs.process_data()
UCIs.upload_data()

coldata.crawler.Kaggle().process_data()
coldata.crawler.Kaggle().upload_data()
