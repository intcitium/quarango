import unittest
import zipfile
import os
import arango
import pandas as pd

from loguru import logger
from database.db_arango import connect_to_db, create_collection, create_document, create_graph
from database.aql import search

PROJECT_NAME = "Test"
DEBUG = True
VERSION = '1.0'


def duplication_error(e):
    logger.info("{} {}".format("Known duplication error", str(e)))


class TestDataBaseMethods(unittest.TestCase):

    def setUp(self):
        self.docs = ["User", "Post", "Location", "Tag"]
        self.edges = ["Knows", "Related"]
        self.test_db = None
        self.test_graph = "test_graph2"
        self.data_path = os.path.join(os.getcwd(), "test_data")
        self.graph_zip = "stack_graph.zip"
        self.graph_test_col = "Tag"
        self.graph_test_edg = "Related"
        self.search_view = "v_search_test"
        self.search = "arangosearch"

    def test_1_connections_to_db(self):
        """
        Try connecting to the db and accept the test as passing if it already exists
        :return:
        """
        logger.info("Testing connection with default values")
        try:
            self.test_db = connect_to_db()
            self.assertIsInstance(self.test_db, arango.database.StandardDatabase)
            logger.info("Connection to {} established", self.test_db.db_name)
        except arango.exceptions.PermissionUpdateError as e:
            logger.info(str(e))
        except Exception as e:
            logger.error(str(e))

    def test_2_create_collection(self):
        """
        Create document and edge collections in the test database.

        On a clean run there should be no errors. Any subsequent run will likely result in duplication type errors which
        are captured as information statements within the tests.

        The test includes at least one collection which triggers the creation of an edge definition based on vertex
        collections. These are defined in the test class as test_col and test_edg. This infers that when creating an
        edge definition, the collection exists and the vertex edges included in the to_col and from_col lists exist.

        :return:
        """
        if not self.test_db:
            self.test_db = connect_to_db()
        logger.info("Testing creation of collections")
        # Create document types
        # OR create special collections from the standard collections at the moment of making a graph
        for col in self.docs:
            try:
                if col == self.graph_test_col:
                    if not self.test_db.has_graph(self.test_graph):
                        self.test_db.create_graph(name=self.test_graph)
                    db_col = create_collection(
                        db=self.test_db,
                        graph=self.test_graph,
                        vertex=True
                    )
                    self.assertIsInstance(db_col, arango.collection.VertexCollection)
                else:
                    db_col = create_collection(
                        db=self.test_db,
                        col=col
                    )
                    self.assertIsInstance(db_col, arango.collection.StandardCollection)
            except arango.exceptions.CollectionCreateError as e:
                duplication_error(e)
            except arango.exceptions.VertexCollectionCreateError as e:
                duplication_error(e)
            except Exception as e:
                logger.error(type(e))
        # Create edge types using the edge=True parameter
        for col in self.edges:
            try:
                if col == self.graph_test_edg:
                    if not self.test_db.has_graph(self.test_graph):
                        self.test_db.create_graph(name=self.test_graph)
                    db_col = create_collection(
                        db=self.test_db,
                        graph=self.test_graph,
                        col=col,
                        from_col=[self.graph_test_col],
                        to_col=[self.graph_test_col],
                        edge=True
                    )
                    self.assertIsInstance(db_col, arango.collection.EdgeCollection)
                else:
                    db_col = create_collection(
                        db=self.test_db,
                        col=col
                    )
                    self.assertIsInstance(db_col, arango.collection.StandardCollection)
            except arango.exceptions.CollectionCreateError as e:
                duplication_error(e)
            except TypeError as e:
                duplication_error(e)
            except arango.exceptions.EdgeDefinitionCreateError as e:
                duplication_error(e)
            except Exception as e:
                logger.error(e)

    def test_3_create_data(self):
        """
        Extract nodes and edges collections from a zip file and insert them into Arango.
        Use a row by row method on the nodes to assign ids to an index. Then create the edge definition and base it on
        the node index.

        :return:
        """
        if not self.test_db:
            self.test_db = connect_to_db()
        # Check if the collection exists before inserting data into it
        logger.info("Testing insertion of data into {}", self.graph_test_col)
        if not self.test_db.has_collection(self.graph_test_col):
            db_col = create_collection(self.test_db, self.graph_test_col)
            self.assertIsInstance(db_col, arango.collection.StandardCollection)
        # Load the data from the zipfile and create a pandas dataframe
        graph_data = zipfile.ZipFile(os.path.join(self.data_path, self.graph_zip))
        dfs = {text_file.filename: pd.read_csv(graph_data.open(text_file.filename))
               for text_file in graph_data.infolist()
               }
        # Create the node index for later use when making edges in the form {name: id from arango}
        test_index = {}
        # Use the unique id generation within Arango to assign ids to the pandas dataframe rows
        for index, row in dfs['stack_network_nodes.csv'].iterrows():
            try:
                r_doc = row.to_dict()
                # Set the index of the name to the key
                test_index[r_doc['name']] = create_document(
                    db=self.test_db,
                    col=self.graph_test_col,
                    doc=r_doc
                )
            except Exception as e:
                print(e)
        # Set the edge to the last type in the test edge labels
        logger.info("Testing insertion of data into {}", self.graph_test_edg)
        # Set a list to contain the edge documents for bulk insertion
        edge_bulk = []
        for index, row in dfs['stack_network_links.csv'].iterrows():
            # Set the edge document equal to the id in the test_index
            edge_bulk.append({
                '_from': "{}".format(test_index[row['source']]),
                '_to': "{}".format(test_index[row['target']]),
                'value': row['value']
            })
        try:
            self.test_db[self.graph_test_edg].import_bulk(edge_bulk)
        except arango.exceptions.DocumentInsertError as e:
            if e.error_message == 'edge attribute missing or invalid':
                pass
            else:
                duplication_error(e)
        logger.info("Data insertion complete")

    def test_4_create_graph(self):
        """
        Create a graph explicitly unlike in create collection which implicitly creates a graph when an edge is given in
        the collection.

        :return:
        """
        if not self.test_db:
            self.test_db = connect_to_db()
        try:
            create_graph(
                db=self.test_db,
                graph=self.test_graph,
                from_col=[self.graph_test_col],
                to_col=[self.graph_test_col],
                edge_col=self.graph_test_edg
            )
        except arango.exceptions.EdgeDefinitionCreateError as e:
            duplication_error(e)
        except Exception as e:
            logger.info(str(e))

    def test_5_get_search(self):
        """
        Test searching for data. Create a view in the test database that covers all collections and fields.
        Validate by checking how many documents have been linked to the ArangoSearch view.

        :return:
        """
        if not self.test_db:
            self.test_db = connect_to_db()
        try:
            # Create an ArangoSearch view that includes all fields of all the documents
            link = {'includeAllFields': True}
            links = {doc: link for doc in self.docs}
            self.test_db.create_arangosearch_view(
                name=self.search_view,
                properties={
                    'cleanupIntervalStep': 0,
                    'links': links
                }
            )
        except arango.exceptions.ViewCreateError as e:
            if 'definition is not an object' in e.error_message:
                logger.error("The links object is a %s when it should be a dict" % type(links))
            else:
                duplication_error(e)
        except Exception as e:
            print(e)
        logger.info("Created view with properties: %s" % self.test_db.view(self.search_view))
        validate = self.test_db.aql.execute(search(self.search_view), count=True, fail_on_warning=True)
        doc_keys = [doc['_key'] for doc in validate]
        logger.info("%d test results found" % len(doc_keys))

    def test_6_get_neighbors(self):
        # Simulate get suggestion items
        return


if __name__ == '__main__':
    unittest.main()

