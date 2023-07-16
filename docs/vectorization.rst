.. _vectorization:

Vectorization
===================================

Vectorize()
--------------------------------

The ``vectorize()`` function is used to vectorize the specified columns in the DataFrame based on the provided features obtained from the ``getFeatures()`` function. It prepares a machine learning-ready DataFrame by applying the appropriate transformations based on the column's data type and features.

Transformation Strategies
-----------------------------

The function employs different strategies to handle various types of columns:

- **Single Categorical String**: For columns containing a single categorical string, the function applies either string indexing or hashing based on the configured strategy.

- **List of Categorical Strings**: When dealing with columns consisting of a list of categorical strings, the function explodes the list and applies string indexing or hashing based on the configured strategy.

- **Single Non-Categorical String**: Columns with a single non-categorical string are processed by applying Word2Vec embedding after tokenization. Optional stop word removal can also be performed.

- **List of Non-Categorical Strings**: In the case of columns containing a list of non-categorical strings, the function combines the list elements, applies tokenization, optional stop word removal, and Word2Vec embedding.

- **Numeric Type**: For columns of numeric types (integer, long, float, double), both single and list types are handled by either joining or exploding the values.

- **Boolean Type**: Columns of boolean type are cast to integers (0 or 1).

- **Unsupported Data Type**: If a column has an unsupported data type, a ``NotImplementedError`` is raised.

The ``vectorize()`` function provides a flexible and extensible way to vectorize different types of columns based on their data type and features. By leveraging this function, you can easily transform your data into a machine learning-ready format.

Example Usage
-----------------------

1. Prepare the DataFrame
~~~~~~~~~~~~~~~~~~~~~~~~~

We already have a dataframe and features dictionary from last example:

.. dropdown:: Click here for the code

  .. code-block:: python
  
    # Import the required libraries
    from sparkrdfx.feature_engineering import FeatureEngineering

    # Create an instance of FeatureCollection
    featureEngineeringObject=FeatureEngineering()
    # Call the getFeatures function with the Spark DataFrame as input
    df2,features=featureEngineeringObject.getFeatures(spark_df)
    df2.show()

+-----------------------+-----------+-----------------+
| **recipe**            |**calorie**| **category**    |
+-----------------------+-----------+-----------------+
| creamy-orange-cake    |  156.1    | desserts        |
+-----------------------+-----------+-----------------+
| summer-chicken-salads |  243.2    | salad           |
+-----------------------+-----------+-----------------+
| orange-raisin-cake    |  168.2    | desserts        |
+-----------------------+-----------+-----------------+
| alfredo-blue          |  486.7    | main-dish       |
+-----------------------+-----------+-----------------+
| millie-pasquinell...  |  621.9    | meat-and-poultry|
+-----------------------+-----------+-----------------+

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

2. Call Vectorize
~~~~~~~~~~~~~~~~~~~~~
Let's call vectorize function on top of that:

.. code-block:: python

  # Import the required libraries
  from sparkkgml.vectorization import Vectorization

  # Create an instance of Vectorization
  vectorizationObject=Vectorization()

  #here we are calling the vectorize function and digitazing all the columns
  digitized_df=vectorizationObject.vectorize(df2,features)
  digitized_df.show(5)

+-----------------------+-----------+-----------------+
| **recipe**            |**calorie**| **category**    |
+-----------------------+-----------+-----------------+
|  creamy-orange-cake   |  156.1    | 0.0             |
+-----------------------+-----------+-----------------+
| summer-chicken-salads |  243.2    | 6.0             |
+-----------------------+-----------+-----------------+
| orange-raisin-cake    |  168.2    | 0.0             |
+-----------------------+-----------+-----------------+
| alfredo-blue          |  486.7    | 3.0             |
+-----------------------+-----------+-----------------+
| millie-pasquinell...  |  621.9    | 9.0             |
+-----------------------+-----------+-----------------+

As you can see, category feature was digitized.

For more details on using vectorize function and its capabilities, please refer to the documentation.