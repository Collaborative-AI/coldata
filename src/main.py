import yaml
import coldata


def main():
    mode = 'local'
    is_update = True  # set to true for first run when vdb was renewed or new documents inserted in mongodb
    config_path = 'config.yml'
    setup_milvus = True
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    database = coldata.mongodb.MongoDB(mode=mode, **config['mongodb'])

    uci = coldata.crawler.UCI(database, **config['crawler'])
    uci.crawl(is_upload=True)

    kaggle = coldata.crawler.Kaggle(database, **config['crawler'])
    kaggle.crawl(is_upload=True)

    aws = coldata.crawler.AWS(database, **config['crawler'])
    aws.crawl(is_upload=True)

    pwc = coldata.crawler.PapersWithCode(database, **config['crawler'])
    pwc.crawl(is_upload=True)

    opendatalab = coldata.crawler.OpenDataLab(database, **config['crawler'])
    opendatalab.crawl(is_upload=True)

    ieeedp = coldata.crawler.IEEEDataPort(database, **config['crawler'])
    ieeedp.crawl(is_upload=True)

    if setup_milvus:
        vdb = coldata.vdb.VDB(**config['vdb']['milvus'], **config['vdb']['text'], **config['vdb']['model'])
        if is_update or config['vdb']['milvus']['renew']:
            vdb.update(database)
        print(f"Number of entities in collection: {vdb.collection.num_entities}")
        result = vdb.search(database, ['Scene Parsing Benchmark'])

        for i, result_i in enumerate(result):
            print(f"\n=== Query {i + 1} Results ===")
            for rank, (index, entry) in enumerate(result_i.items(), start=1):
                distance = entry.get('distance')
                record = entry.get('record', {})
                url = record.get('URL', 'N/A')
                info = record.get('info', '').replace('\n', ' ').replace('###', '').strip()
                preview = info[:200] + '...' if len(info) > 200 else info

                print(f"[{rank}] Index: {index}")
                print(f"     Distance: {distance:.4f}")
                print(f"     URL: {url}")
                print(f"     Info: {preview}")

        if is_update:
            vdb.drop()
    return


if __name__ == '__main__':
    main()
