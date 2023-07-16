.. _gettingStarted:

Getting Started
==================

Let's start with a basic example, we will retrieve data from a SPARQL endpoint and convert it into a Spark DataFrame using the ``getDataFrame`` function.

.. code-block:: python

   # Import the required libraries
   from sparkkgml.data_acquisition import DataAcquisition

   # Create an instance of KgQuery
   DataAcquisitionObject=DataAcquisition()

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
   spark_df = DataAcquisitionObject.getDataFrame(endpoint=endpoint, query=query)
   spark_df.show()

+------------------------------------------+
| recipe                                   |
+==========================================+
| recipeKG:recipe/peanut-butter-tandy-bars |
+------------------------------------------+
| recipeKG:recipe/the-best-oatmeal-cookies |
+------------------------------------------+
| recipeKG:recipe/peach-cobbler-ii         |
+------------------------------------------+


The ``getDataFrame`` function will query the data from the specified SPARQL endpoint and return a Spark DataFrame that you can use for further analysis or machine learning tasks.

For more details on using SparkKG-ML and its functions, please refer to the documentation.

Notes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


The ``getDataFrame`` function retrieves data from a SPARQL endpoint and converts it into a Spark DataFrame. It follows the following steps:

1. If the ``endpoint`` is not provided, the default endpoint is used. If the default endpoint is not set, an error message is displayed, and the function returns.
2. If the ``query`` is not provided, the default query is used. If the default query is not set, an error message is displayed, and the function returns.
3. The data is queried from the SPARQL endpoint and converted into a Pandas DataFrame.
4. If there are null values in the Pandas DataFrame, handling methods are applied based on the configured amputation method.
5. The Pandas DataFrame is then converted into a Spark DataFrame.
6. The resulting Spark DataFrame is returned.