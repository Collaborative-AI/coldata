# ColData
_**Col**laborative **Data**set Search Engine with Vector Database_

**coldata** is an open-source dataset search engine designed to help researchers, data scientists, developers, and the broader community **collaboratively** discover, share, and access relevant datasets across a variety of sources.  
The engine crawls metadata from popular dataset hosting platforms with **BeautifulSoup**, stores it in **MongoDB**, and transforms it into a vector-based database using **MilvuDB** for enhanced search and retrieval.


## Features

- **Multi-source Crawling**: We gather metadata from major dataset repositories.
  
- **Vector-based Search**: The metadata is converted into vector embeddings using the language model, enabling powerful semantic search capabilities.

- **Interface**: With the help of **Gradio**, we offer a simple demo to interact with the engine and quickly locate datasets.

- **Scalable**: The underlying database, **MongoDB**, and vector engine, **MilvuDB**, ensure that the system scales as the number of crawled datasets grows.

## Datasets

We currently support crawling and indexing datasets from the following sources:

| Dataset Name                          | Number of Datasets    | Completed  |
|---------------------------------------|-----------------------|------------|
| **UCI**                               | 675                   | ✅         |
| **Kaggle**                            | 40,000+               | ✅         |
| **Registry of Open Data on AWS**      | 496                   | ✅         |
| **Papers With Code**                  | 8,966                 | ✅         |
| **Figshare**                          | 1,856,206             |          |
| **Mendeley Data**                     | 1,307,514             |          |
| **Hugging Face Datasets**             | 88,179                |         |
| **Zenodo**                            | 234,972               |          |
| **IEEE Dataport**                     | 8,928                 |         |
| **Open Data Lab**                     | 6,432                 |  ✅        |
| **Roboflow Universe**                 | 200,000+              |          |

## Installation

Clone the repository:

```bash
git clone https://github.com/yourusername/coldata.git
cd coldata
```

Install dependencies:

```bash
pip install -r requirements.txt
```


## Configuration

The system can be customized via the `config.yml` file, where you can configure hyperparameters for both MongoDB and MilvuDB.


## Quick Start

1. **Start MongoDB**: 

   Run `start_mongo.sh` to start a local MongoDB instance.

   ```bash
   ./start_mongo.sh
   ```

2. **Start Milvus DB**:

   Run `manage_milvus.sh` to start the Milvus vector database.

   ```bash
   ./manage_milvus.sh
   ```

3. **Run Scheduler**: 

   Set up the scheduler to crawl datasets at a specified interval by running:

   ```bash
   python scheduler.py
   ```

4. **Demo Interface**: 

   To quickly test the dataset search, you can use the Gradio-based demo:

   ```bash
   python demo.py
   ```
   
## How to Contribute

We welcome contributions to improve **coldata**. If you have ideas for new features, bug fixes, or dataset sources to include, please feel free to open an issue or submit a pull request.


## License

This project is licensed under the MIT License.
