# VDB (First Draft)

A short intro on how to use the Files.

## Table of Contents

- [VDB](#vdb-(first-draft))
  - [Table of Contents](#table-of-contents)
  - [Features](#features)
  - [usage](#usage)
  - [Files & Functions](#files-&-functions)
  - [License](#license)

## Features


## Usage

1. Run the two .py files with --config arguments


## Files & Functions

- `data_processor.py`: 
  - 'load_data': load all data from mongodb (needs fixing)
  - 'convert_to_document': fix data structures for further ingestion
  - 'split_texts': split the text fields to chunks
  - 'create_embed_model': create model for later embeddings
  - 'create_vector_store': save data to Milvus with embed model, only need one of this or "embed" (needs fixing)
  - 'embed': embed data but not saving to Milvus, only need one of this or "create_vector_store" (needs fixing)
  - 'backup_embeddings': upload the ouput of 'embed' to mongodb without checking duplication (needs fixing)
- `vector_database.py`: 
  - 'load_embeddings': load backup embeddings from mongodb (needs fixing)
  - 'connect_to_docker': for windows
  - 'create_vdb': create a collection in Milvus with the four needed fields without checking duplications(needs fixing)
  - 'recover_vdb': check collection exists
  - 'load_collection': load collection
  - 'search': search algorithm
  - 'release': release the collection

## License

