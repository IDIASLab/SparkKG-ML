.. _kg:

Knowledge Graphs
==================

This module provides functionalities to manipulate and interact with RDF-based Knowledge Graphs (KG) using Apache Spark and GraphFrames. The KG class allows users to parse RDF data, create vertices and edges, and construct a GraphFrame for further analysis.

The KG class in this module is designed to represent and manipulate a Knowledge Graph (KG) from RDF data. It provides methods to parse RDF data and convert it into a Spark-compatible format, as well as create a GraphFrame for graph-based operations.

Example Usage
------------------------

*** Basic Usage: Creating a Knowledge Graph *** 

In this example, we will create a Knowledge Graph from an RDF file and convert it into a GraphFrame using the `KG` class.

.. code-block:: python

    # Import the required libraries
    from kg import KG
    from pyspark.sql import SparkSession

    # Initialize a Spark session (optional)
    spark = SparkSession.builder.appName("KG-Example").getOrCreate()

    # Specify the location of the RDF file
    rdf_location = "path_to_your_rdf_file.rdf"

    # Create an instance of the KG class
    kg = KG(location=rdf_location, fmt='turtle', sparkSession=spark)

    # Create the GraphFrame
    graph_frame = kg.createKG()

    # Display the vertices with the additional outgoing edges column
    graph_frame.vertices.show()

*** Extra Feature: Directly Accessing Vertices and Edges DataFrames ***

For more advanced use cases, you can directly access the vertices and edges DataFrames using the `getKGasDataFrame` method. This is an optional step, as the `createKG` method already handles the creation of these DataFrames internally.

.. code-block:: python

    # Retrieve the DataFrames for vertices and edges
    edges_df, vertices_df = kg.getKGasDataFrame()

    # Display the edges and vertices DataFrames
    edges_df.show()
    vertices_df.show()

In this example, make sure to replace `"path_to_your_rdf_file.rdf"` with the actual path to your RDF file and adjust the format as necessary (e.g., 'turtle', 'xml').

Conclusion
----------------------------------

The `KG` class in this module provides an efficient way to parse RDF data and create a Knowledge Graph in a Spark environment. It is highly customizable, allowing you to skip specific predicates and handle both local and remote RDF sources. By converting the RDF data into Spark DataFrames and leveraging GraphFrames, you can perform advanced graph-based operations on your Knowledge Graph.

For further customization and advanced usage, please refer to the API documentation.

