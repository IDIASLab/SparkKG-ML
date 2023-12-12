.. _dataAcquisition:

Data Acquisition
==================
This use case demonstrates how to retrieve data from a SPARQL endpoint and convert it into a Spark DataFrame using the ``getDataFrame`` function in SparkKG-ML.
Additionaly, you also have the option to utilize ``query_local_rdf`` for querying a local RDF file.

The ``getDataFrame`` function retrieves data from a SPARQL endpoint and converts it into a Spark DataFrame. It follows the following steps:

1. If the ``endpoint`` is not provided, the default endpoint is used. If the default endpoint is not set, an error message is displayed, and the function returns.
2. If the ``query`` is not provided, the default query is used. If the default query is not set, an error message is displayed, and the function returns.
3. The data is queried from the SPARQL endpoint and converted into a Pandas DataFrame.
4. If there are null values in the Pandas DataFrame, handling methods are applied based on the configured amputation method.
5. The Pandas DataFrame is then converted into a Spark DataFrame.
6. The resulting Spark DataFrame is returned.

Example Usage
------------------------

In this example, we will retrieve data from a SPARQL endpoint and convert it into a Spark DataFrame using the ``getDataFrame`` function.

.. code-block:: python

   # Import the required libraries
   from sparkkgml.data_acquisition import DataAcquisition

   # Create an instance of KgQuery
   dataAcquisitionObject=DataAcquisition()

   # Specify the SPARQL endpoint and query
   endpoint = "https://recipekg.arcc.albany.edu/RecipeKG"
   query ="""
        PREFIX schema: <https://schema.org/>
        PREFIX recipeKG:<http://purl.org/recipekg/>
        SELECT  ?recipe
        WHERE { ?recipe a schema:Recipe. }
        LIMIT 3
        """

   # Retrieve the data as a Spark DataFrame
   spark_df = dataAcquisitionObject.getDataFrame(endpoint=endpoint, query=query)
   spark_df.show()

   # Perform further operations on the Spark DataFrame
   # ...

Make sure to replace ``"https://recipekg.arcc.albany.edu/RecipeKG"`` with your SPARQL endpoint URL and with your desired SPARQL query.

The ``getDataFrame`` function will query the data from the specified SPARQL endpoint and return a Spark DataFrame that you can use for further analysis or machine learning tasks.

Remember to handle any potential errors or null values according to your requirements.

Error: Handling Null Values
~~~~~~~~~~~~~~~~~~~~~~~~~~~

In this example, we will demonstrate how null values in data can lead to errors when using the ``getDataFrame`` function.

.. code-block:: python

    # Import the required libraries
    from sparkkgml.data_acquisition import DataAcquisition

    # Create an instance of RDFX
    dataAcquisitionObject=DataAcquisition()

    # Set the SPARQL endpoint URL and query
    endpoint = "http://example.com/sparql"
    query = "SELECT * WHERE { ?s ?p ?o } LIMIT 100"

    # Retrieve data from the SPARQL endpoint and apply nullReplacement
    spark_df = dataAcquisitionObject.getDataFrame(endpoint=endpoint, query=query)

    # Display the resulting Spark DataFrame
    spark_df.show()


If there are null values in the retrieved data and no handling method is specified, a ``TypeError`` will be raised.

Example Error Message:

    ``TypeError``: If there are null values in the Pandas DataFrame and no handling method is specified.

To avoid this error, you need to handle null values in your data appropriately using the ``nullReplacement`` or ``nullDrop`` methods provided by the RDFX library.


Null Value Handling
------------------------------

The RDFX library provides two methods for handling null values in data: `nullReplacement` and `nullDrop`.

- ``nullReplacement()``: This method replaces null values in a DataFrame with specified values based on different scenarios.

- ``nullDrop()``: This method drops columns and rows from a DataFrame based on specified thresholds for null values.


In this example, we will demonstrate how to retrieve data from a SPARQL endpoint and apply null value handling methods using the SparkKG-ML library.

Scenario 1: Null Drop
~~~~~~~~~~~~~~~~~~~~~~~~

In this scenario, we will use the ``nullDrop()`` method with custom thresholds for dropping columns and rows with null values.

.. code-block:: python

    # Import the required libraries
    from sparkkgml.data_acquisition import DataAcquisition

    # Create an instance of RDFX
    dataAcquisitionObject=DataAcquisition()

    # Set the SPARQL endpoint URL and query
    endpoint = "https://dbpedia.org/sparql"
    query = "SELECT * WHERE { ?s ?p ?o } LIMIT 100"

    # Configure nullDrop with custom thresholds
    dataAcquisitionObject.set_amputationMethod("nullDrop")
    dataAcquisitionObject.set_columnNullDropPercent(50)
    dataAcquisitionObject.set_rowNullDropPercent(30)

    # Retrieve data from the SPARQL endpoint and apply nullDrop
    spark_df = dataAcquisitionObject.getDataFrame(endpoint=endpoint, query=query)

    # Display the resulting Spark DataFrame
    result_df.show()

If there are still null values after dropping columns and rows, the ``nullReplacement`` method will be called automatically.

Scenario 2: Null Replacement
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In this scenario, we will use the ``nullReplacement`` method with custom values for handling null values.

.. code-block:: python

    # Import the required libraries
    from sparkkgml.data_acquisition import DataAcquisition

    # Create an instance of RDFX
    dataAcquisitionObject=DataAcquisition()

    # Set the SPARQL endpoint URL and query
    endpoint = "https://dbpedia.org/sparql"
    query = "SELECT * WHERE { ?s ?p ?o } LIMIT 100"

    # Configure nullReplacement with custom values
    dataAcquisitionObject.set_nullReplacementMethod = "customValue"
    dataAcquisitionObject.set_customValueVariable = 0
    dataAcquisitionObject.set_customStringValueVariable = "unknown"

    # Retrieve data from the SPARQL endpoint and apply nullReplacement
    spark_df = dataAcquisitionObject.getDataFrame(endpoint=endpoint, query=query)

    # Display the resulting Spark DataFrame
    result_df.show()

Additional Options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Customizing the behavior of the ``nullReplacement`` and ``nullDrop`` methods in the Data Acquisition class:

- ``nullReplacement``: You can change the following variables to customize the behavior:

  - `_nullReplacementMethod`
  - `_customValueVariable`
  - `_customStringValueVariable`

- ``nullDrop``: You can change the following variables to customize the behavior:

  - `_amputationMethod`
  - `_columnNullDropPercent`
  - `_rowNullDropPercent`

Adjust these variables according to your specific requirements to control the null value handling behavior in your data processing pipeline.

Conclusion
----------------------------------

The SparkKG-ML library provides flexible methods for handling null values in data. By using the ``nullReplacement`` and ``nullDrop`` methods, you can preprocess your data effectively and ensure quality in your analysis.

For more detailed information on each method and its parameters, please refer to the API documentation.


