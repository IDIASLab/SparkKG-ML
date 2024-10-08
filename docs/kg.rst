.. _kg:

Knowledge Graphs
==================

This module provides functionalities to manipulate and interact with RDF-based Knowledge Graphs (KG) using Apache Spark and GraphFrames. The KG class allows users to parse RDF data, create vertices and edges, and construct a GraphFrame for further analysis.


**Note**: To use this module, you need to install the `graphframes` library. `GraphFrames` is a package for Apache Spark that provides DataFrame-based Graphs. It is essential for creating and manipulating graphs in this module.

For installation instructions, please refer to the official `GraphFrames` documentation: 
`GraphFrames Installation Guide <https://graphframes.github.io/graphframes/docs/_site/index.html#installation>`_.

Example Usage
------------------------

**Basic Usage: Creating a Knowledge Graph**

In this example, we will create a Knowledge Graph from an RDF file and convert it into a GraphFrame using the `KG` class.

.. code-block:: python

    # Import the required libraries
    from kg import KG
    from pyspark.sql import SparkSession

    # Initialize a Spark session (optional)
    spark = SparkSession.builder.appName("KG-Example").getOrCreate()

    # Specify the location of the RDF file
    rdf_location = "path_to_your_rdf_file.rdf"

    # Define predicates to skip during RDF parsing (optional)
    skip_predicates = {"http://example.org/skipThisPredicate"}

    # Create an instance of the KG class
    kg = KG(location=rdf_location, fmt='turtle', skip_predicates=skip_predicates, sparkSession=spark)

    # Create the GraphFrame
    graph_frame = kg.createKG()

**Note:** The skip_predicates parameter allows you to exclude specific predicates from the RDF parsing process, which can help streamline your Knowledge Graph by ignoring irrelevant or unnecessary relationships.

**Directly Accessing Vertices and Edges DataFrames**

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

