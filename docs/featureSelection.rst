.. _featureSelection:

Feature Selection
=====================

The Feature Selection module offers various techniques for selecting relevant features from your dataset. It includes implementations of popular feature selection methods, such as forward sequential feature selection and correlation feature selection, as well as extensibility for introducing additional feature selection algorithms not covered by Spark.

This module aims to simplify the process of feature selection in PySpark by providing a unified interface and a set of intuitive functions. It leverages the capabilities of Spark MLlib to efficiently handle large-scale datasets and allows users to select the most informative features for their machine learning pipelines.

The Feature Selection module currently includes the following feature selection methods:

1. Forward Sequential Feature Selection (FSFS)
2. Correlation Feature Selection (CFS)
3. ... (Other feature selection methods to be introduced)

The module is designed to be flexible, allowing users to easily extend it with their own feature selection algorithms by following a common interface.

Correlation Feature Selection
-----------------------------

The Correlation Feature Selection (CFS) method is one of the feature selection techniques available in the Feature Selection module. It identifies highly correlated features in the dataset and selects only those features that are not highly correlated with each other.

Example Usage
~~~~~~~~~~~~~~

To illustrate the correlation feature selection function, first let's get the nutritional information of recipes.

.. dropdown:: Click here for the code

   .. code-block:: python

        # Import the required libraries
        from sparkkgml.data_acquisition import DataAcquisition
        from pyspark.sql.functions import regexp_replace

        # Create an instance of KgQuery
        dataAcquisitionObject=DataAcquisition()

        # Specify the SPARQL endpoint and query
        endpoint = "https://recipekg.arcc.albany.edu/RecipeKG"
        query = """
          PREFIX schema: <https://schema.org/>
          PREFIX recipeKG:<http://purl.org/recipekg/>
          PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
          SELECT DISTINCT ?recipe ?calorie ?fat ?saturatedFat ?cholesterol ?carbohydrate ?fiber ?protein ?sugar
           WHERE {
                   ?recipe a schema:Recipe.
                   ?recipe recipeKG:hasNutritionalInformation ?a.

                   ?a recipeKG:hasCalorificData ?b.
                   ?b recipeKG:hasAmount ?calorie.

                   ?a recipeKG:hasFatData ?c.
                   ?c recipeKG:hasAmount ?fat.

                   ?a recipeKG:hasSaturatedFatData ?d.
                   ?d recipeKG:hasAmount ?saturatedFat.

                   ?a recipeKG:hasCholesterolData ?e.
                   ?e recipeKG:hasAmount ?cholesterol.

                   ?a recipeKG:hasCarbohydrateData ?f.
                   ?f recipeKG:hasAmount ?carbohydrate.

                   ?a recipeKG:hasFiberData ?g.
                   ?g recipeKG:hasAmount ?fiber.

                   ?a recipeKG:hasProteinData ?h.
                   ?h recipeKG:hasAmount ?protein.

                   ?a recipeKG:hasSugarData ?k.
                   ?k recipeKG:hasAmount ?sugar. 
                    }
            """

        spark_df = dataAcquisitionObject.getDataFrame(endpoint=endpoint, query=query)
        spark_df.show()



+---------------------+-----------+-------+----------------+---------------+---------------+---------+-----------+---------+
|       **recipe**    |**calorie**|**fat**|**saturatedFat**|**cholesterol**|**carbohyrate**|**fiber**|**protein**|**sugar**|
+=====================+===========+=======+================+===============+===============+=========+===========+=========+
| peanut-butter-tan.  |  230.0    |  9.5  |      3.2       |     31.8      |      32.7     |   1.6   |    5.8    |  22.9   |
+---------------------+-----------+-------+----------------+---------------+---------------+---------+-----------+---------+
| the-best-oatmeal-.  |  172.8    |  7.6  |      3.6       |     29.1      |       24.8    |    1.1  |     2.4   |   14.1  |
+---------------------+-----------+-------+----------------+---------------+---------------+---------+-----------+---------+
| peach-cobbler-ii    |   672.4   |  24.0 |       8.5      |     18.5      |     112.7     |   1.0   |    4.7    |  88.2   |
+---------------------+-----------+-------+----------------+---------------+---------------+---------+-----------+---------+
| pie-crust-v         |  210.4    |  12.1 |       7.4      |     39.2      |      22.6     |   0.6   |    2.9    |   4.8   |
+---------------------+-----------+-------+----------------+---------------+---------------+---------+-----------+---------+
| palak-paneer-indi.  |   315.1   |  20.7 |      4.5       |     17.0      |      16.3     |   5.5   |   19.9    |   3.5   |
+---------------------+-----------+-------+----------------+---------------+---------------+---------+-----------+---------+


We are interested in selecting features that are not highly correlated, so we can focus on the most relevant and independent variables for our analysis.

For this example, we have the following features available: 'calorie', 'fat', 'saturatedFat', 'cholesterol', 'carbohydrate', 'fiber', 'protein', and 'sugar'. We want to identify and retain the features that are not strongly correlated with each other.

To perform the correlation feature selection, we can use the following code snippet:

.. code-block:: python

    # import correlation_feature_selection from feature_selection module
    from sparkkgml.feature_selection import correlation_feature_selection

    features_arr = ['calorie', 'fat', 'saturatedFat', 'cholesterol', 'carbohydrate', 'fiber', 'protein', 'sugar']
    spark_df = correlation_feature_selection(spark_df, 0.5, features_arr)
    spark_df.show()

In this example, we pass the DataFrame, ``spark_df``, as the input, along with the correlation threshold of 0.5 and the list of features, ``features_arr``, that we want to analyze.

The correlation feature selection function, ``correlation_feature_selection``, will calculate the correlation matrix among the specified features. It will identify pairs of features that have a correlation coefficient exceeding the threshold (0.5 in this case).

During the analysis, it is determined that 'fat' and 'saturatedFat' have a strong positive correlation above the threshold. In this case, we would retain only one of these features, as they provide similar information. The resulting DataFrame, ``new_df``, will contain the selected features that are not highly correlated.

Finally, we display the resulting DataFrame using the ``show()`` method to visually inspect the filtered features.

+---------------------+----------------+---------+-----------+---------+
|       **recipe**    |**saturatedFat**|**fiber**|**protein**|**sugar**|
+=====================+================+=========+===========+=========+
| peanut-butter-tan.  |      3.2       |   1.6   |    5.8    |  22.9   |
+---------------------+----------------+---------+-----------+---------+
| the-best-oatmeal-.  |      3.6       |    1.1  |     2.4   |   14.1  |
+---------------------+----------------+---------+-----------+---------+
| peach-cobbler-ii    |       8.5      |   1.0   |    4.7    |  88.2   |
+---------------------+----------------+---------+-----------+---------+
| pie-crust-v         |       7.4      |   0.6   |    2.9    |   4.8   |
+---------------------+----------------+---------+-----------+---------+
| palak-paneer-indi.  |      4.5       |   5.5   |   19.9    |   3.5   |
+---------------------+----------------+---------+-----------+---------+


It's important to note that this example showcases a simplified scenario with a small amount of data. In real-world cases, correlation analysis can uncover valuable insights, helping us identify redundant or highly correlated features, and improve the accuracy and interpretability of our machine learning models.

For a comprehensive understanding of the correlation feature selection method and its parameters and more feature selection methods please refer to the API documentation.
