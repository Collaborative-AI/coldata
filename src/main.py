import os
import coldata


def main():
    attempts = 1
    mongodb_key_path = os.path.join('key', 'mongodb.txt')
    UCIs = coldata.crawler.UCI(mongodb_key_path, attempts=attempts)
    UCIs.crawl()
    UCIs.upload()

    # coldata.crawler.Kaggle().process_data()
    # coldata.crawler.Kaggle().upload_data()


if __name__ == '__main__':
    main()
