import yaml
import coldata


def main():
    mode = 'local'
    if_update = False  # set to true for first run
    if_drop = False
    config_path = 'config.yml'
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)

    database = coldata.mongodb.MongoDB(mode=mode, **config['mongodb'])

    uci = coldata.crawler.UCI(database, **config['crawler'])
    uci.crawl()
    uci.upload()

    kaggle = coldata.crawler.Kaggle(database, **config['crawler'])
    kaggle.crawl()
    kaggle.upload()

    vdb = coldata.vdb.VDB(**config['vdb']['milvus'], **config['vdb']['text'], **config['vdb']['model'])
    if if_update or config['vdb']['milvus']['renew']:
        vdb.update(database)
    result = vdb.search(database, ['best dataset'])
    for i in range(len(result)):
        for record in result[i]:
            print(record)

    if if_drop:
        vdb.drop()
    return


if __name__ == '__main__':
    main()
