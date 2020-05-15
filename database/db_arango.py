from loguru import logger

import arango


def connect_to_db(db_name='test', username='root', password='admin', hosts='http://localhost:8529'):
    """
    Expect a root level user to access the database and perform transactions

    :param db_name: str
        default to test

    :param username: str
        default to root

    :param password: str
        default to admin

    :param hosts: str
        default to http://localhost:8529

    :return: client.db
        client that allows requests to the Arango system database. This is the root db to create other databases.
        arango.database.StandardDatabase
    """
    client = arango.ArangoClient(hosts=hosts)
    sys_db = client.db('_system', username=username, password=password)
    if not sys_db.has_database(db_name):
        sys_db.create_database(
            name=db_name
        )
    return client.db(db_name, username=username, password=password)


def create_collection(db=None, col='Test', edge=False, vertex=False, from_col=None, to_col=None, graph='Test'):
    """
    Create an Arango collection based on a variety of inputs to simplify an API access in a micro services architecture.
    The basic collections should be documents that can be changed into edges and/or vertex collections. Graphs can be
    created on the fly using the Arango method of managing them with edge collections defined by their attributes _to
    and _from collections based on vertex collections. if there is an edge collection then there should also be a
    definition that consists of the following requirements
        {
            'edge_collection': 'edge_collection_name',
            'from_vertex_collections': ['from_vertex_collection_name'],
            'to_vertex_collections': ['to_vertex_collection_name']
        }
    otherwise if there is a vertex then it must be created in the graph and lastly if there is nothing then just
    create the document

    :param db: arango.db
        the database that will be added to the server
    :param col: string
        name for the collection
    :param edge: bool
        switch to create an edge collection
    :param vertex: bool
        switch to create a vertex collection
    :param from_col: list of strings
        list of vertex names that have are also in the edge definitions
    :param to_col: list of strings
        list of vertex names that have are also in the edge definitions
    :param graph: string
        name for the graph
    :return: collection or edge defintion
        1 of 3 types of collections based on the variables received

    """
    # Case that an edge is True which requires the definition of from and to collections within vertices
    if edge and from_col and to_col:
        return db.graph(graph).create_edge_definition(
            edge_collection=col,
            from_vertex_collections=from_col,
            to_vertex_collections=to_col
        )
    # Case that an vertex is True which makes it available to the edge collections
    elif vertex:
        return db.graph(graph).create_vertex_collection(name=col)
    # Case of a standard collection which can be later referenced as edge or
    else:
        return db.create_collection(name=col)


def create_document(db=None, doc=None, col=None):
    """
    Insert a document into a database collection and return the document id for potential use in creating
    other documents such as edges. The id is a combination of the key and the collection in the form
    <collection/key>. Run quality checks such as data types and collection existence.

    :param db: arango.database.StandardDatabase
        target for the new document that contains the collection
    :param doc: json
        document in JSON format or if edge includes the _to and _from keys for an edge
    :param col:
        target collection for the new document
    :return:
    """
    if isinstance(doc, dict) and isinstance(col, str) and isinstance(db, arango.database.StandardDatabase):
        if db.has_collection(col):
            return db[col].insert(doc)['_id']
        else:
            return None
    else:
        return None


def create_graph(db=None, graph=None, from_col=None, to_col=None, edge_col=None):
    """
    Based on a database that should not be the _sys db, create a graph and return it so it can be used for follow on
    functions.

    :param db:
    :param graph: str
        the name of the graph which if not existing will be used to create it
    :param from_col:
        list of existing collections that are have ids contained within the edge definition within the sources or from
    :param to_col:
        list of existing collections that are have ids contained within the edge definition within the targets or to
    :param edge_col:
        name of the edge definition collection
    :return: arango.graph
    """
    if (isinstance(graph, str) and isinstance(from_col, list) and isinstance(to_col, list)
            and isinstance(edge_col, str) and isinstance(db, arango.database.StandardDatabase)):
        if not db.has_graph(graph):
            graph_db = db.create_graph(graph)
        else:
            graph_db = db.graph(graph)
        graph_db.create_edge_definition(
            edge_collection=edge_col,
            from_vertex_collections=from_col,
            to_vertex_collections=to_col
        )
        return graph_db

    return None


def close_db_connection(client):
    logger.info("Closing connection to database")
    client.db


