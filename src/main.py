import yaml
import coldata


def main():
    mode = 'local'
    config_path = 'config.yml'
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)

    uci = coldata.crawler.UCI(key=config['mongodb'][mode], num_attempts=config['crawl']['num_attempts'],
                              **config['crawl']['dataset']['uci'])
    # uci.crawl()
    # uci.upload()
    #
    # kaggle = coldata.crawler.Kaggle(key=config['key'][mode], num_attempts=config['crawl']['num_attempts'],
    #                                 **config['crawl']['dataset']['kaggle'])
    # kaggle.crawl()
    # kaggle.upload()

    vdb = coldata.vdb.VDB(**config['vdb']['milvus'], **config['vdb']['text'], **config['vdb']['model'])
    vdb.update(uci)
    result = vdb.search(uci, ['best dataset'])
    for i in range(len(result)):
        for record in result[i]:
            print(record)
    return


if __name__ == '__main__':
    main()
