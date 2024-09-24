# coldata

## Overview
`coldata` is a Python package designed for data crawling, processing, and integration with MongoDB and MilvusDB. It streamlines the acquisition of datasets from various sources and facilitates efficient data management.

## Requirements
To run `coldata`, you need to have the following installed:

- Python 3.x
- MongoDB
- Milvus

You can install the required Python packages using the `requirements.txt` file:

```bash
pip install -r requirements.txt
```

Make sure to have your MongoDB and Milvus instances running and properly configured.

## Configuration
Edit the `config.yml` file to specify your MongoDB and crawler settings. This file is essential for configuring connections and parameters.

## Usage
Run the main execution file to start the data crawling process:

```bash
python main.py
```

This script:
- Initializes the MongoDB connection.
- Crawls data from UCI and Kaggle.
- Updates the MilvusDB with the crawled data and performs searches.

