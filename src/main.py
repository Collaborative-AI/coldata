import yaml
import coldata


def main():
    mode = 'local'
    config_path = 'config.yml'
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    uci = coldata.crawler.UCI(key=config['key'][mode], num_attempts=config['crawl']['num_attempts'],
                              **config['crawl']['dataset']['uci'])
    uci.crawl()
    uci.upload()

    # kaggle = coldata.crawler.Kaggle(mongodb_key_path, attempts=attempts)
    # kaggle.crawl()
    # kaggle.upload()
    return


if __name__ == '__main__':
    main()
