import yaml
import coldata
import gradio as gr


# Wrap the search function
def search_engine(search_keywords):
    mode = 'local'
    config_path = 'config.yml'

    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)

    # Connect to MongoDB
    database = coldata.mongodb.MongoDB(mode=mode, **config['mongodb'])

    # Initialize Milvus vector database (vdb)
    vdb = coldata.vdb.VDB(**config['vdb']['milvus'], **config['vdb']['text'], **config['vdb']['model'])

    # Perform the search using the given search keywords
    result = vdb.search(database, [search_keywords])

    # Return search results
    return result


# Gradio UI function
def gradio_search_interface(search_term):
    search_results = search_engine(search_term)
    results = "\n".join([str(result) for result in search_results])
    return results


# Gradio interface
search_input = gr.inputs.Textbox(label="Search for Dataset")
search_output = gr.outputs.Textbox(label="Search Results")

demo = gr.Interface(fn=gradio_search_interface, inputs=search_input, outputs=search_output,
                    title="Dataset Search Engine")

# Launch the app
# demo.launch()
demo.launch(server_name="0.0.0.0", server_port=7860, share=False)
