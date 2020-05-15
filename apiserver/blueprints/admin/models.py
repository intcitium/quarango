from database.db_arango import connect_to_db
from database.aql import search, neighbors

client = connect_to_db()


def get_suggestions(search_term='sample'):
    results = client.aql.execute(search(search_term), count=True, fail_on_warning=True)
    return [doc for doc in results]


def get_neighbors(node_key='sample'):
    """
    Get all neighbors which are defined by a single depth of traversal in either direction
    Return the result as a graph formatted for visualization which requires nodes and edges in their own
    lists, nodes with keys, and lines that consist of source and target. No source or target should have
    a key that doesn't exist in the nodes

    :param node_key: str
        ID that is the collection/key of the node being searched
    :return: graph: dict
        standard json object that can be translated into any number of visualization libraries
    """
    results = client.aql.execute(neighbors(node_key), count=True, fail_on_warning=True)
    graph = {'index': [], 'nodes': [], 'lines': []}
    for r in results:
        if 'v' in list(r.keys()):
            if r['v']['_id'] not in graph['index']:
                # Copy the whole result and make the 'key' from the system _id
                node = r['v']
                node['key'] = r['v']['_id']
                # Pop out system attributes
                node.pop('_rev')
                node.pop('_key')
                node.pop('_id')
                graph['index'].append(node['key'])
                graph['nodes'].append(node)
        if 'e' in list(r.keys()):
            edge = {
                'source': r['e']['_from'],
                'target': r['e']['_to'],
                'label': str(r['e']['_id'])[:str(r['e']['_id']).find('/')]}
            graph['lines'].append(edge)
    return graph
