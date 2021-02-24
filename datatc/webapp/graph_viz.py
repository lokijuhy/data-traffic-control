import dash
import dash_cytoscape as cyto
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output
from datatc import DataDirectory
from datatc.data_directory import SelfAwareDataDirectory, DataFile


dd = DataDirectory.load('viz_tree')

elements = [
    {'data': {'id': 'one', 'label': 'Node 1'}, 'position': {'x': 75, 'y': 75}},
    {'data': {'id': 'two', 'label': 'Node 2'}, 'position': {'x': 200, 'y': 200}},
    {'data': {'source': 'one', 'target': 'two'}}
]


def remove_data_head(p, data_head):
    return str(p).replace(data_head, '')


data_head = str(dd.path)
G = []
node_metadata = {}
data_file_nodes = []
for file_name in dd.contents:
    x = dd.contents[file_name]
    x_path = remove_data_head(x.path, data_head)
    if type(x) in [DataFile, SelfAwareDataDirectory]:
        G.append({'data': {'id': x_path, 'label': file_name}})
        metadata = {'type': type(x)}
        if type(x) == SelfAwareDataDirectory:
            metadata['transform_steps'] = x.get_info()['transform_steps']
        elif type(x) == DataFile:
            data_file_nodes.append(x_path)
        node_metadata[x_path] = metadata
        if type(x) == SelfAwareDataDirectory:
            transform_steps = x.get_info()['transform_steps']
            for step in reversed(transform_steps):
                if 'file_path' in step:
                    source_file = remove_data_head(step['file_path'], data_head)
                    G.append({'data': {'source': source_file, 'target': x_path}})
                    break
elements = G

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets,
                meta_tags=[{"name": "viewport", "content": "width=device-width"}])

app.layout = html.Div(
    [
        html.Div([
                html.Div([
                        html.H1('✈️ data traffic control'),
                        dcc.Markdown('Data Directory: `{}`'.format(data_head)),
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
                            style={'width': '100%', 'height': '1000px'},
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
        )
    ]
)


@app.callback(Output('provenance', 'children'),
              Input('cytoscape-data-viz', 'tapNodeData'))
def displayClickNodeData(data):
    if data:
        metadata = node_metadata[data['id']]
        type = metadata['type'].__name__
        metadata_str = "### {}".format(data['label'])
        metadata_str += "\n*{}*".format(type)

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

                # for key in step:
                #     if key == 'code':
                #         metadata_str += '\n```python\n{}\n```'.format(step[key])
                #     else:
                #         metadata_str += '\n * {}: `{}`'.format(key, step[key])
        return metadata_str


if __name__ == '__main__':
    app.run_server(debug=True)
