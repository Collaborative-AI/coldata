# Coldata

Coldata is an application designed for searching datasets using a vector database by crawling the metadata of datasets online from various sources. We use BeautifulSoup, MongoDB, Sentence-BERT, and Milvus to achieve this.

## Table of Contents

- [Coldata](#coldata)
    - [Features](#features)
    - [Usage](#usage)
    - [Files & Functions](#files--functions)
    - [Contributing](#contributing)
    - [License](#license)

## Features

- Crawl metadata from various online sources.
- Store and manage data using MongoDB.
- Embed dataset metadata using Sentence-BERT.
- Use Milvus for efficient vector-based search.

## Usage

1. Add the `mongodb.txt` from Google Drive and put it in the `crawler` folder.
2. Run `main.py` in the `crawler` folder to start crawling.
3. Run the .py files with `--config` arguments to use the vector database functionalities.

## Files & Functions

### Crawler

A basic OOP modularized crawler for UCI and Kaggle.

- `main.py`: Main file to start the crawler.

### VDB

- `milvus_vdb.py`:
    - `load_data`: Load all data from MongoDB.
    - `convert_to_document`: Fix data structures for further ingestion.
    - `split_texts`: Split the text fields into chunks and update MongoDB.
    - `create_embed_model`: Create model for embeddings.
    - `embed`: Embed data but not save to Milvus.
    - `load_embeddings`: Load backup embeddings from MongoDB (needs fixing).
    - `connect_to_docker`: For Windows.
    - `update_vdb`: Create a collection in Milvus with the four needed fields.
    - `recover_vdb`: Check if the collection exists.
    - `load_collection`: Load collection.
    - `search`: Search algorithm.
    - `release`: Release the collection.
- `chunk_count.txt`: Record updated chunk ID numbers.
