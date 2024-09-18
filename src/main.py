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

    vdb = coldata.vdb.VDB(config['vdb']['host'], config['vdb']['port'], config['vdb']['model']['model_name'],
                          config['vdb']['text']['chunk_size'], config['vdb']['text']['chunk_overlap'],
                          config['vdb']['text']['add_start_index'], config['vdb']['model']['normalize_embeddings'],
                          config['vdb']['model']['device'])
    vdb.process(uci)
    return


if __name__ == '__main__':
    main()
