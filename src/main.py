import yaml
import coldata


def main():
    mode = 'local'
    is_update = True  # set to true for first run when vdb was renewed or new documents inserted in mongodb
    config_path = 'config.yml'
    setup_milvus = False
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    database = coldata.mongodb.MongoDB(mode=mode, **config['mongodb'])

    uci = coldata.crawler.UCI(database, **config['crawler'])
    uci.crawl(is_upload=True)

    kaggle = coldata.crawler.Kaggle(database, **config['crawler'])
    exit()
    kaggle.crawl(is_upload=True)

    aws = coldata.crawler.AWS(database, **config['crawler'])
    aws.crawl(is_upload=True)

    pwc = coldata.crawler.PapersWithCode(database, **config['crawler'])
    pwc.crawl(is_upload=True)

    opendatalab = coldata.crawler.OpenDataLab(database, **config['crawler'])
    opendatalab.crawl(is_upload=True)

    if setup_milvus:
        vdb = coldata.vdb.VDB(**config['vdb']['milvus'], **config['vdb']['text'], **config['vdb']['model'])
        if is_update or config['vdb']['milvus']['renew']:
            vdb.update(database)
        result = vdb.search(database, ['Satellite Computed Bathymetry Assessment-SCuBA'])
        for i in range(len(result)):
            for index in result[i]:
                print(result[i][index]['distance'])
                print(result[i][index])

        if is_update:
            vdb.drop()
    return


if __name__ == '__main__':
    main()
