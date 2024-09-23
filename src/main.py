import yaml
import coldata


def main():
    mode = 'local'
    if_crawl = True
    config_path = 'config.yml'
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)

    database = coldata.mongodb.MongoDB(mode=mode, **config['mongodb'])

    uci = coldata.crawler.UCI(database, **config['crawler'])
    if if_crawl:
        uci.crawl()
        uci.upload()

    kaggle = coldata.crawler.Kaggle(database, **config['crawler'])
    if if_crawl:
        kaggle.crawl()
        kaggle.upload()

    vdb = coldata.vdb.VDB(**config['vdb']['milvus'], **config['vdb']['text'], **config['vdb']['model'])
    vdb.update(database)
    result = vdb.search(database, ['best dataset'])
    for i in range(len(result)):
        for record in result[i]:
            print(record)
    return


if __name__ == '__main__':
    main()
