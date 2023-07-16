.. _featureEngineering:

Feature Engineering
===================

.. _example-usage-get-features:

Example Usage: getFeatures
--------------------------

This example demonstrates how to use the ``getFeatures`` function to extract features and their descriptions from a DataFrame.

1. Prepare the DataFrame
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Import the required libraries
   from sparkkgml.data_acquisition import DataAcquisition
   from pyspark.sql.functions import regexp_replace

   # Create an instance of KgQuery
   dataAcquisitionObject=DataAcquisition()

   # Specify the SPARQL endpoint and query
   endpoint = "https://recipekg.arcc.albany.edu/RecipeKG"
   query ="""
        PREFIX schema: <https://schema.org/>
        PREFIX recipeKG:<http://purl.org/recipekg/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT DISTINCT ?recipe ?calorie ?category
        WHERE {
                    ?recipe a schema:Recipe.

                    ?recipe recipeKG:hasNutritionalInformation ?a.
                    ?a recipeKG:hasCalorificData ?b.
                    ?b recipeKG:hasAmount ?calorie.

                    ?recipe recipeKG:belongsTo ?subcategory.
                    ?subcategory rdfs:subClassOf* ?category.
                    ?category a recipeKG:RecipeCategory.
            }
            LIMIT 200
        """

   # Retrieve the data as a Spark DataFrame
   spark_df = dataAcquisitionObject.getDataFrame(endpoint=endpoint, query=query)
   #let's also delete the url and just have names
   spark_df = spark_df.withColumn("recipe", regexp_replace('recipe','http://purl.org/recipekg/recipe/',''))
   spark_df = spark_df.withColumn("category", regexp_replace('category','http://purl.org/recipekg/categories/',''))
   spark_df = spark_df.withColumn("category", regexp_replace('category','/',''))
   spark_df.show(5)


+---------------------------+-----------+--------------+
| **recipe**                |**calorie**| **category** |
+---------------------------+-----------+--------------+
|  peanut-butter-tandy-bars |  230      |  desserts    |
+---------------------------+-----------+--------------+
|  the-best-oatmeal-cookies |  172.8    |  desserts    |
+---------------------------+-----------+--------------+
|  peach-cobbler-ii         |  672.4    |  desserts    |
+---------------------------+-----------+--------------+
|  pie-crust-v              |  210.4    |  desserts    |
+---------------------------+-----------+--------------+
|  dads-beef-and-chive-dip  |  77.6     |  appetizers..|
+---------------------------+-----------+--------------+


2. Extract Features
~~~~~~~~~~~~~~~~~~~

Next, we can use the ``getFeatures`` function to extract features and their descriptions from the DataFrame.

.. code-block:: python

   # Import the required libraries
   from sparkrdfx.feature_engineering import FeatureEngineering

   # Create an instance of FeatureCollection
   featureEngineeringObject=FeatureEngineering()

   # Call the getFeatures function with the Spark DataFrame as input
   df2,features=featureEngineeringObject.getFeatures(spark_df)
   print(features)

The ``getFeatures`` function returns a tuple containing the collapsed DataFrame (`collapsedDataFrame`) and a dictionary of feature descriptions (`featureDescriptions`).

- `featureDescriptions`:


    'calorie': {'featureType': 'Single_NonCategorical_Double',
      'name': 'calorie',
      'nullable': False,
      'datatype': DoubleType,
      'numberDistinctValues': 193,
      'isListOfEntries': False,
      'isCategorical': False},
    'category': {'featureType': 'Single_Categorical_String',
      'name': 'category',
      'nullable': False,
      'datatype': StringType,
      'numberDistinctValues': 15,
      'isListOfEntries': False,
      'isCategorical': True}


3. Analyze the Results
~~~~~~~~~~~~~~~~~~~~~~

The analysis of the results involves two main components:

1. ``collapsedDataFrame``: This DataFrame represents the unique entities with the extracted features as columns.

2. ``featureDescriptions``: This dictionary provides information about each feature's properties. The keys are the feature column names, and the values are dictionaries with the following information:

   - ``featureType`` (str): The type of the feature, combining information about whether it is a list or a single value, whether it is categorical or non-categorical, and the data type.
   - ``name`` (str): The name of the feature column.
   - ``nullable`` (bool): A flag indicating if the feature can have null values.
   - ``datatype`` (spark.DataType): The data type of the feature column.
   - ``numberDistinctValues`` (int): The number of distinct values in the feature column.
   - ``isListOfEntries`` (bool): A flag indicating if the feature is a list of entries.
   - ``isCategorical`` (bool): A flag indicating if the feature is categorical.

These components provide valuable insights into the structure and properties of the resulting DataFrame, allowing you to analyze and further process the vectorized data.

``collapsedDataFrame`` contains the transformed DataFrame with unique entities and their corresponding feature columns.

``featureDescriptions`` offers detailed information about each feature, enabling you to understand the characteristics of the extracted features and their data types.