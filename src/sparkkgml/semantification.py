#!/usr/bin/env python
# coding: utf-8

# In[30]:


from rdflib import Graph, Literal, Namespace, URIRef,BNode
from datetime import datetime


# In[2]:


class Semantification:
    """
    The Semantification class is used to semantify data by creating an RDF graph and serializing it to a destination file.

    Attributes:
    - _graph (rdflib.Graph): The RDF graph to store the semantified data.

    Methods:
    - __init__(): Initializes the Semantification class by creating an empty RDF graph.
    - get_graph(): Retrieves the RDF graph.
    - set_graph(graph): Sets the RDF graph to the specified graph.
    - semantify(df, uri1='uri', label1='label', prediction1='prediction', dest='experiment.ttl'): Semantifies the given DataFrame by adding the data to the RDF graph and serializing it.
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
   

    
    def semantify(self, df, uri1='uri', label1='label', prediction1='prediction', dest='experiment.ttl'):
        
        """
        Semantifies the given DataFrame by creating an RDF graph and serializing it to a destination file.

        Parameters:
        - df (pyspark.sql.DataFrame): The DataFrame containing the data to semantify.
        - uri1 (str): The column name in the DataFrame representing the URI of each instance.
        - label1 (str): The column name in the DataFrame representing the label of each instance.
        - prediction1 (str): The column name in the DataFrame representing the prediction of each instance.
        - dest (str): The destination file path where the RDF graph will be serialized. Default is 'experiment.ttl'.

        Returns:
        None
        
        """
        
        # Define namespaces
        EX_NS = Namespace("http://example.com/")
        self._graph.bind("my", EX_NS)
        
        # Define experiment type properties
        experimentTypePropertyURIasString = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
        experimentTypeURIasString = "https://example/Vocab/experiment"
        # Create experiment node
        metagraphDatetime = datetime.now()
        experimentHash = str(hash(str(metagraphDatetime)))
        experimentNode = BNode(experimentHash)
        # Create experiment type properties nodes
        experimentTypePropertyNode = URIRef(experimentTypePropertyURIasString)
        experimentTypeNode = URIRef(experimentTypeURIasString)

        # Add experiment type triple
        self._graph.add((experimentNode, experimentTypePropertyNode, experimentTypeNode))
       
        for row in df.rdd.collect():
            # Get the values from the DataFrame row
            uri = row[uri1]
            label = row[label1]
            prediction = row[prediction1]

            # Create a URIRef for the instance node
            instance_node = URIRef(uri)

            # Add the instance node to the graph
            #graph.add((instance_node, RDF.type, FOAF.Person))

            # Add the label and prediction relations
            self._graph.add((instance_node, EX_NS.label, Literal(label)))
            self._graph.add((instance_node, EX_NS.prediction, Literal(prediction)))
            self._graph.add((experimentNode, EX_NS.hasPrediction, Literal(prediction)))
            
        
        self._graph.serialize(destination=dest)      

