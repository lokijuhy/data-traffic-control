import argparse
import dash
import dash_cytoscape as cyto
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output, State
from datatc.data_directory import SelfAwareDataDirectory, DataFile, DataDirectory
from typing import Dict, List, Tuple

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, title='datatc', external_stylesheets=external_stylesheets,
                meta_tags=[{"name": "viewport", "content": "width=device-width"}])


def remove_data_dir_prefix(p, prefix):
    p = str(p)
    prefix = str(prefix)
    return p.replace(prefix, '')


def dir_to_graph(dd: DataDirectory, graph_elements: List, node_metadata: Dict, root_nodes: List, data_dir_head: str
                 ) -> Tuple[List, Dict, List]:
    """
    Recursive function to turn a directory into a graph.

    Args:
        dd: DataDirectory to characterize into a graph.
        graph_elements: List defining the graph nodes and edges
        node_metadata: Dictionary of metadata about each node to display upon click
        root_nodes: List of node names that should be root nodes (DataFiles)
        data_dir_head: Head of the data directory (must be the same for all levels of recursion!)

    Returns: graph elements list, node_metadata dictionary, and list of root nodes

    """
    for file_name in dd.contents:
        x = dd.contents[file_name]
        x_path = remove_data_dir_prefix(x.path, prefix=data_dir_head)
        if type(x) == DataDirectory:
            graph_elements, node_metadata, root_nodes = dir_to_graph(x, graph_elements, node_metadata, root_nodes,
                                                                     data_dir_head)
        elif type(x) in [DataFile, SelfAwareDataDirectory]:
            graph_elements.append({'data': {'id': x_path, 'label': file_name}})
            metadata = {'type': type(x).__name__}
            if type(x) == SelfAwareDataDirectory:
                metadata['transform_steps'] = x.get_info()['transform_steps']
            elif type(x) == DataFile:
                root_nodes.append(x_path)
            node_metadata[x_path] = metadata
            if type(x) == SelfAwareDataDirectory:
                transform_steps = x.get_info()['transform_steps']
                for step in reversed(transform_steps):
                    if 'file_path' in step:
                        source_file = remove_data_dir_prefix(step['file_path'], prefix=data_dir_head)
                        graph_elements.append({'data': {'source': source_file, 'target': x_path}})
                        break
    return graph_elements, node_metadata, root_nodes


def data_dir_to_graph(dd: DataDirectory) -> Tuple[List, Dict, List]:
    """
    Recursive function to turn a directory into a graph.

    Args:
        dd: DataDirectory to characterize into a graph.

    Returns: graph elements list, node_metadata dictionary, and list of root nodes

    """
    graph_elements = []
    node_metadata = {}
    root_nodes = []
    data_dir_head = str(dd.path)

    graph_elements, node_metadata, root_nodes = dir_to_graph(dd, graph_elements, node_metadata, root_nodes,
                                                             data_dir_head)
    return graph_elements, node_metadata, root_nodes


def get_app_layout(elements, node_metadata, data_file_nodes, data_dir):
    return html.Div(
        [
            html.Div([
                    html.Div([
                            html.H1('✈️ data traffic control'),
                            dcc.Markdown('Data Directory: `{}`'.format(data_dir)),
                        ],
                        className="pretty_container twelve columns",
                    ),

                ],
                className="row flex-display",
            ),
            html.Div(
                [
                    html.Div(
                        [
                            cyto.Cytoscape(
                                id='cytoscape-data-viz',
                                layout={
                                    'name': 'breadthfirst',
                                    'roots': data_file_nodes,
                                },
                                style={'width': '100%', 'height': '500px'},
                                elements=elements
                            ),
                        ],
                        className="pretty_container six columns",
                        # style={"border": "2px black solid"}
                    ),
                    html.Div(
                        [
                            dcc.Markdown(id='provenance')
                        ],
                        className="pretty_container six columns"
                    )
                ],
                className="row flex-display"
            ),
            dcc.Store(id='node_metadata', data=node_metadata)
        ]
    )


@app.callback(
    Output('provenance', 'children'),
    [Input('cytoscape-data-viz', 'tapNodeData'),
     State('node_metadata', 'data'),
     ])
def displayClickNodeData(data, node_metadata):
    if data:
        metadata = node_metadata[data['id']]
        d_type = metadata['type']
        metadata_str = "### {}".format(data['label'])
        metadata_str += "\n*{}*".format(d_type)

        if 'transform_steps' in metadata:
            for i, step in enumerate(metadata['transform_steps']):
                metadata_str += '\n\n---\n'
                metadata_str += '\n##### Step {}\n'.format(i)
                if 'file_path' in step.keys():
                    metadata_str += '\n {}: `{}`'.format('file_path', step['file_path'])
                if 'timestamp' in step.keys():
                    metadata_str += '\n {} | {}'.format(step['timestamp'], '#'+step['git_hash'])
                if 'code' in step.keys():
                    metadata_str += '\n```python\n{}\n```'.format(step['code'])
                if 'kwargs' in step.keys() and step['kwargs'] != {}:
                    metadata_str += '\n {}: `{}`'.format('kwargs', step['kwargs'])

        return metadata_str


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('data_dir', help='Data directory to view. Must be previously registered with datatc.')
    args = parser.parse_args()

    dd = DataDirectory.load(args.data_dir)
    data_dir_path = str(dd.path)
    graph_elements, node_metadata, root_nodes = data_dir_to_graph(dd)

    layout = get_app_layout(graph_elements, node_metadata, root_nodes, data_dir=data_dir_path)
    app.layout = layout
    app.run_server(debug=True)


if __name__ == '__main__':
    main()
