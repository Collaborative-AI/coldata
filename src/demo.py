import yaml
import coldata
import gradio as gr
import os

os.environ["no_proxy"] = "localhost,127.0.0.1,::1"
filtered_keys = ['_id', 'index']


# Function to filter out unwanted keys
def filter_result(results):
    filtered_results = []
    for record in results:
        # Ensure each record is treated as a dictionary and filter out unwanted keys
        filtered_record = {key: value for key, value in record.items() if key not in filtered_keys}
        filtered_results.append(filtered_record)
    return filtered_results


# Wrap the search function
def search_engine(database, vdb, search_keywords):
    # Perform the search using the given search keywords
    results = vdb.search(database, [search_keywords])
    result = results[0]
    # Filter out unnecessary fields (_id, index)
    filtered_results = filter_result(result)
    return filtered_results


# Function to format the dataset into Markdown
def format_results_as_markdown(results):
    formatted_results = ""

    for dataset in results:
        for key, value in dataset.items():
            # Format key and value in Markdown
            formatted_results += f"**{key.capitalize()}**: {value}\n\n"
        formatted_results += "---\n\n"  # Divider between datasets

    return formatted_results


def main():
    mode = 'local'
    if_update = False
    config_path = 'config.yml'
    debug = True
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)

    # Connect to MongoDB
    database = coldata.mongodb.MongoDB(mode=mode, **config['mongodb'])

    # Initialize Milvus vector database (vdb)
    vdb = coldata.vdb.VDB(**config['vdb']['milvus'], **config['vdb']['text'], **config['vdb']['model'])
    if if_update:
        vdb.update(database)

    # Gradio UI function
    def gradio_search_interface(search_term):
        # Get the search results from the engine
        results = search_engine(database, vdb, search_term)

        # Convert results to formatted markdown
        formatted_results = format_results_as_markdown(results)

        return formatted_results

    # Gradio interface
    search_input = gr.Textbox(label="Search for Dataset")
    search_output = gr.Markdown(label="Search Results")

    demo = gr.Interface(fn=gradio_search_interface, inputs=search_input, outputs=search_output,
                        title="Dataset Search Engine", description="Search for datasets and view their details.")

    # Launch the app
    demo.launch(server_name="127.0.0.1", server_port=7860, share=False, debug=debug)
    return


if __name__ == '__main__':
    main()
