import yaml
import coldata
import gradio as gr
import os

os.environ["no_proxy"] = "localhost,127.0.0.1,::1"


# Wrap the search function
def search_engine(database, vdb, search_keywords):
    # Perform the search using the given search keywords
    results = vdb.search(database, [search_keywords])
    parsed_results = []
    for i in range(len(results)):
        parsed_results.append([])
        for record in results[i]:
            parsed_results[i].append(record)
    return parsed_results


def main():
    mode = 'local'
    config_path = 'config.yml'
    debug = True

    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)

    # Connect to MongoDB
    database = coldata.mongodb.MongoDB(mode=mode, **config['mongodb'])

    # Initialize Milvus vector database (vdb)
    vdb = coldata.vdb.VDB(**config['vdb']['milvus'], **config['vdb']['text'], **config['vdb']['model'])
    # vdb.update(database)

    # Gradio UI function
    def gradio_search_interface(search_term):
        results = search_engine(database, vdb, search_term)
        results = results[0]
        results = "\n".join([str(result) for result in results])
        return results

    # Gradio interface
    search_input = gr.Textbox(label="Search for Dataset")
    search_output = gr.Textbox(label="Search Results")

    demo = gr.Interface(fn=gradio_search_interface, inputs=search_input, outputs=search_output,
                        title="Dataset Search Engine")

    # Launch the app
    # demo.launch()
    demo.launch(server_name="127.0.0.1", server_port=7860, share=False, debug=debug)
    return


if __name__ == '__main__':
    main()
