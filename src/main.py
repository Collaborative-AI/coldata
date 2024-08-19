import os
import coldata


def main():
    attempts = 1
    mongodb_key_path = os.path.join('key', 'mongodb.txt')

    uci = coldata.crawler.UCI(mongodb_key_path, attempts=attempts)
    uci.crawl()
    uci.upload()

    # kaggle = coldata.crawler.Kaggle(mongodb_key_path, attempts=attempts)
    # kaggle.crawl()
    # kaggle.upload()
    return


if __name__ == '__main__':
    main()
