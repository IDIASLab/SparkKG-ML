import rdflib
from rdflib import Literal
from typing import Set
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

from graphframes import GraphFrame


class KG:
    """
    A class to represent and manipulate a Knowledge Graph (KG) from RDF data.

    Attributes:
        location (str): The location of the RDF file, either local or remote (HTTP/HTTPS).
        _is_remote (bool): Indicates if the RDF source is remote. Defaults to False.
        fmt (str): The format of the RDF file (e.g., 'xml', 'n3', 'turtle'). Defaults to None, which infers the format.
        skip_predicates (Set[str]): A set of predicates to skip during RDF parsing.
        key_to_vertex_hashMap (dict): A hashmap mapping integer keys to vertices (subject/object).
        vertex_to_key_hashMap (dict): A hashmap mapping vertices (subject/object) to integer keys.
        sparkSession (SparkSession): The Spark session to be used for DataFrame creation.

    Methods:
        getKGasDataFrame():
            Parses the RDF data and converts it into vertices and edges DataFrames compatible with GraphFrames.

        createKG():
            Creates and returns a GraphFrame from the vertices and edges DataFrames, 
            with an additional column indicating if a vertex has outgoing edges.
    """

    def __init__(self, location: str, _is_remote: bool = False, fmt: str = None, skip_predicates: Set[str] = set(), sparkSession: SparkSession = None):
        """
        Initializes the KG object with RDF data location, format, and optional SparkSession.

        Args:
            location (str): The location of the RDF file.
            _is_remote (bool): Whether the RDF source is remote (HTTP/HTTPS). Defaults to False.
            fmt (str): The format of the RDF file (e.g., 'xml', 'n3', 'turtle'). Defaults to None.
            skip_predicates (Set[str]): A set of predicates to skip during RDF parsing. Defaults to an empty set.
            sparkSession (SparkSession): An optional Spark session to be used. If not provided, a new session is created.
        """
        self.location = location
        self._is_remote = _is_remote 
        self.fmt = fmt 
        self.skip_predicates = skip_predicates
        self.key_to_vertex_hashMap = {}
        self.vertex_to_key_hashMap = {}

        # Initialize SparkSession
        if sparkSession is None:
            # If no custom SparkSession is provided, create a new one.
            self.sparkSession = SparkSession.builder.getOrCreate()
        else:
            # Use the provided SparkSession.
            self.sparkSession = sparkSession

    def getKGasDataFrame(self):
        """
        Parses the RDF data from the specified location and converts it into two DataFrames: one for vertices 
        and one for edges, which are compatible with GraphFrames.

        Returns:
            tuple: A tuple containing the edges DataFrame and the vertices DataFrame.
        """
        triple_list = []  # List to store RDF triples as (subject, object, predicate) tuples
        vertex_set = set()  # Set to store unique vertices (subjects and objects)

        if self.location is not None:
            # Determine if the location is remote (HTTP/HTTPS) or local
            self._is_remote = self.location.startswith("http://") or self.location.startswith("https://")
            if self._is_remote:
                print('The RDF source is remote')
                # Additional logic for handling remote RDF files would go here
            else:
                idNum = 0  # Counter for generating unique integer keys for vertices
                for subj, pred, obj in rdflib.Graph().parse(self.location, format=self.fmt):
                    # Skip predicates that are in the skip_predicates set
                    if str(pred) not in self.skip_predicates:
                        # Check if the object is a literal
                        if isinstance(obj, Literal):
                            new_obj = str(obj.value)
                        else:
                            new_obj = str(obj)

                        # Add object to hashmaps if not already present
                        if new_obj not in self.vertex_to_key_hashMap:
                            self.vertex_to_key_hashMap[new_obj] = idNum
                            self.key_to_vertex_hashMap[idNum] = new_obj
                            idNum += 1

                        # Add subject to hashmaps if not already present
                        if str(subj) not in self.vertex_to_key_hashMap:
                            self.vertex_to_key_hashMap[str(subj)] = idNum
                            self.key_to_vertex_hashMap[idNum] = str(subj)
                            idNum += 1

                        # Add predicate to hashmaps if not already present
                        if str(pred) not in self.vertex_to_key_hashMap:
                            self.vertex_to_key_hashMap[str(pred)] = idNum
                            self.key_to_vertex_hashMap[idNum] = str(pred)
                            idNum += 1

                        # Add the triple to the list
                        triple_list.append((self.vertex_to_key_hashMap[str(subj)], self.vertex_to_key_hashMap[new_obj], self.vertex_to_key_hashMap[str(pred)]))
                        vertex_set.add(self.vertex_to_key_hashMap[new_obj])
                        vertex_set.add(self.vertex_to_key_hashMap[str(subj)])

        # Define the schema for the edges DataFrame
        schema = StructType([
            StructField("src", IntegerType(), True),
            StructField("dst", IntegerType(), True),
            StructField("relationship", IntegerType(), True)
        ])
        edge_dataframe = self.sparkSession.createDataFrame(triple_list, schema=schema)

        # Define the schema for the vertices DataFrame
        vertices_schema = StructType([StructField("id", IntegerType(), True)])
        vertices_dataframe = self.sparkSession.createDataFrame(((vertex,) for vertex in vertex_set), vertices_schema)

        return edge_dataframe, vertices_dataframe

    def createKG(self):
        """
        Creates a GraphFrame from vertices and edges DataFrames with an additional column in the vertices DataFrame
        indicating whether a vertex has any outgoing edges.

        Returns:
            GraphFrame: A GraphFrame object created from the vertices and edges DataFrames.
        """
        # Fetch the vertices and edges DataFrames
        edges, vertices = self.getKGasDataFrame()

        # Add a column to the vertices DataFrame to indicate if a vertex has outgoing edges
        vertices_with_outgoing_edges = vertices.join(
            edges,
            vertices["id"] == edges["src"],
            "left_outer"
        ).select(
            vertices["id"],  # Keep the original vertex ID
            f.when(f.col("dst").isNotNull(), True).otherwise(False).alias("has_outgoing_edge")  # Check for outgoing edges
        )

        # Ensure the vertices DataFrame is distinct to avoid duplicates
        vertices_with_outgoing_edges = vertices_with_outgoing_edges.distinct()

        # Create and return the GraphFrame
        return GraphFrame(vertices_with_outgoing_edges, edges)
    


        
    
