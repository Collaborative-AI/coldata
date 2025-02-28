import yaml
import coldata


def main():
    mode = 'local'
    if_update = False  # set to true for first run when vdb was renewed or new documents inserted in mongodb
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

    aws = coldata.crawler.AWS(database, **config['crawler'])
    aws.crawl()
    aws.upload()
    '''
    pwc = coldata.crawler.PapersWithCode(database, **config['crawler'])
    pwc.crawl()
    pwc.upload()
    '''
    vdb = coldata.vdb.VDB(**config['vdb']['milvus'], **config['vdb']['text'], **config['vdb']['model'])
    if if_update or config['vdb']['milvus']['renew']:
        vdb.update(database)
    result = vdb.search(database, ['Satellite Computed Bathymetry Assessment-SCuBA'])
    for i in range(len(result)):
        for index in result[i]:
            print(result[i][index]['distance'])
            print(result[i][index]['record'])

    if if_drop:
        vdb.drop()
    return


if __name__ == '__main__':
    main()
