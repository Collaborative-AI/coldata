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

1. Run the .py file with --config arguments


## Files & Functions

- `milvus_vdb.py`: 
  - 'load_data': load all data from mongodb 
  - 'convert_to_document': fix data structures for further ingestion
  - 'split_texts': split the text fields to chunks, update mongodb
  - 'create_embed_model': create model for later embeddings
  - 'embed': embed data but not saving to Milvus
  - 'load_embeddings': load backup embeddings from mongodb (needs fixing)
  - 'connect_to_docker': for windows
  - 'update_vdb': create a collection in Milvus with the four needed fields
  - 'recover_vdb': check collection exists
  - 'load_collection': load collection
  - 'search': search algorithm
  - 'release': release the collection
- `chunk_count.txt`: record updated chunkid numbers
## License

