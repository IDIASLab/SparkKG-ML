from rdflib import Graph, Literal, Namespace, URIRef,BNode
from datetime import datetime


class Semantification:
    """
        The Semantification class is designed to semantify data by creating an RDF graph and serializing it to a destination file.
    
        Attributes:
        - _graph (rdflib.Graph): The RDF graph used to store the semantified data.
    
        Methods:
        - __init__(): Initializes the Semantification class by creating an empty RDF graph.
        - get_graph(): Retrieves the RDF graph.
        - set_graph(graph): Sets the RDF graph to the specified graph.
        - semantify(df, namespace, exp_uri='uri', exp_label='label', exp_prediction='prediction', dest='experiment.ttl'):
            Semantifies the given DataFrame by adding the data to the RDF graph and serializing it.

    """
       
    def __init__(self):     
        # Create an RDF graph
        self._graph = Graph()
        
    # getter functions
    def get_graph(self):
        return self._graph
    
    # setter functions
    def set_graph(self, graph):
        self._graph= graph
   

    
    def semantify(self, df, namespace, exp_uri='uri', exp_label='label', exp_prediction='prediction', dest='experiment.ttl'):
        """
            Semantifies the given DataFrame by creating an RDF graph and serializing it to a destination file.
        
            Parameters:
            - df (pyspark.sql.DataFrame): The DataFrame containing the data to semantify.
            - namespace (str): The namespace string used to define RDF namespaces for the experiment.
            - exp_uri_column (str): The column name in the DataFrame representing the URI of each instance.
            - exp_label_column (str): The column name in the DataFrame representing the label of each instance.
            - exp_prediction_column (str): The column name in the DataFrame representing the prediction of each instance.
            - dest (str): The destination file path where the RDF graph will be serialized. Default is 'experiment.ttl'.
        
            Returns:
            None
        """
        
        # Define namespaces
        EX_NS = Namespace(namespace)
        self._graph.bind("experiment", EX_NS)
        
        # Define experiment type properties
        experimentTypePropertyURIasString = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
        #experimentTypeURIasString = "https://example/Vocab/experiment"
        # Create experiment node
        #metagraphDatetime = datetime.now()
        #experimentHash = str(hash(str(metagraphDatetime)))
        #experimentNode = BNode(experimentHash)
        
        # Create experiment type properties nodes
        #experimentTypePropertyNode = URIRef(experimentTypePropertyURIasString)
        #experimentTypeNode = URIRef(experimentTypeURIasString)

        # Add experiment type triple
        #self._graph.add((experimentNode, experimentTypePropertyNode, experimentTypeNode))
       
        for row in df.rdd.collect():
            # Get the values from the DataFrame row
            uri = row[exp_uri]
            label = row[exp_label]
            prediction = row[exp_prediction]

            # Create a URIRef for the instance node
            instance_node = URIRef(uri)

            # Add the label and prediction relations
            self._graph.add((instance_node, EX_NS.label, Literal(label)))
            self._graph.add((instance_node, EX_NS.prediction, Literal(prediction)))
            #self._graph.add((experimentNode, EX_NS.hasPrediction, Literal(prediction)))
            
        
        self._graph.serialize(destination=dest)      

