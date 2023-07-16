.. _featureAugmentation:

Feature Augmentation
====================

Introduction
------------

The `Feature Augmentation` module in the SparkKG-ML library is based on the `kgextension <https://kgextension.readthedocs.io/en/latest/>`_ module in Python and serves as a linker between this library and Spark. It enables the use of SparkKG-ML functionalities within Spark by encapsulating the functions in the `kgextension` library. This module facilitates linking datasets to public knowledge graphs and extracting features from those graphs in PySpark.

By leveraging the `Feature Augmentation` module, users can seamlessly integrate their data processing workflows with public knowledge graphs such as DBpedia, WikiData, or the EU Open Data Portal. The module provides functionalities for linking datasets to any Linked Open Data (LOD) source, generating new features from LOD sources, performing hierarchy-based feature selection algorithms, and integrating features from different sources.

The combination of RDFX and Spark empowers users to enhance their data analysis and machine learning tasks by incorporating contextual information from public knowledge graphs. It opens up new opportunities for data integration, feature engineering, and knowledge-driven insights.

Usage Examples
--------------

To showcase the capabilities of the `Feature Augmentation` module, let's introduce two usage example functions.

In the following sections, we will dive into these usage examples, providing step-by-step instructions and code snippets to illustrate the functionality of the `Feature Augmentation` module.

DBpedia Lookup Linker
~~~~~~~~~~~~~~~~~~~~~~

The ``spark_dbpedia_lookup_linker`` function is a utility function provided by the `Feature Augmentation` module. It accesses the DBpedia Lookup web service to look up DBpedia URIs by related keywords. The lookup is based on either matching the label of a resource or matching frequently used anchor text from Wikipedia. The results are ranked by the number of inlinks pointing from other Wikipedia pages at a result page. See the `DBpediaLookupAPI` for more information.


In this example, we will demonstrate how to use the ``spark_dbpedia_lookup_linker`` function to link a Spark DataFrame column to DBpedia resources using the DBpedia Lookup API.

Let's first create our example dataframe:

.. dropdown:: Click here for the code

    .. code-block:: python

       data = [
                   ('Germany',),
                   ('Italy',),
                   ('United States of America',),
               ]

       spark_df = spark.createDataFrame(data, ['country'])
       spark_df.show()

+---------------------------+
| **country**               |
+---------------------------+
|  Germany                  |
+---------------------------+
|  Italy                    |
+---------------------------+
| United States of America  |
+---------------------------+


4. Apply the DBpedia lookup linker function:

.. code-block:: python

   #import spark_dbpedia_lookup_linker function from spark_kgextension module
   from sparkrdfx.feature_augmentation import spark_dbpedia_lookup_linker 

   df_lookup_linked = spark_dbpedia_lookup_linker(
           spark_df, column="country", new_attribute_name="new_link",
           query_class="", max_hits=1, lookup_api="KeywordSearch")
   df_lookup_linked.show()

The ``spark_dbpedia_lookup_linker`` function is called with the Spark DataFrame `spark_df`, specifying the column to be linked (`countries`), and providing optional parameters such as the new attribute name (`new_link`), query class, maximum hits, and lookup API. The function performs the DBpedia lookup and returns a new DataFrame `df_lookup_linked` with an additional column `new_link` containing the DBpedia URIs associated with the country names. Calling `show()` displays the resulting DataFrame:

+---------------------------+--------------------------------------+
| **country**               |                          **new_link**|
+---------------------------+--------------------------------------+
|  Germany                  |  http://dbpedia.org/resource/G...    |
+---------------------------+--------------------------------------+
|  Italy                    |  http://dbpedia.org/resource/I...    |
+---------------------------+--------------------------------------+
|  United States of America |  http://dbpedia.org/resource/U...    |
+---------------------------+--------------------------------------+

The DataFrame `df_lookup_linked` shows the original 'countries' column along with the linked URIs from DBpedia.


Specific Relation Generator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The specific relation generator creates attributes from a specific direct relation. The following example uses the default parameter settings:

In this example, we will demonstrate how to use the ``spark_specific_relation_generator`` function to link a Spark DataFrame column to DBpedia resources using the DBpedia Lookup API.

Let's first create our example dataframe:

.. dropdown:: Click here for the code

    .. code-block:: python
        

       # Define the schema for the DataFrame
        schema = StructType([
            StructField("country", StringType(), True),
            StructField("link", StringType(), True)
        ])

        # Create the data as a list of tuples
        data = [
            ("Spain", "http://dbpedia.org/resource/Spain"),
            ("Japan", "http://dbpedia.org/resource/Japan"),
            ("Chile", "http://dbpedia.org/resource/Chile")
        ]
        # Create the DataFrame
        df = spark.createDataFrame(data, schema)

+---------------------------+
| **country**               |
+---------------------------+
|  Spain                    |
+---------------------------+
|  Japan                    |
+---------------------------+
|  Chile                    |
+---------------------------+


4. Apply the Specific Relation Generator function:

.. code-block:: python

   #import spark_specific_relation_generator function from spark_kgextension module
   from sparkrdfx.feature_augmentation import spark_specific_relation_generator

   df_specific_relation = spark_specific_relation_generator(df, "link")
   df_specific_relation.show()

The `spark_specific_relation_generator` function is called with the Spark DataFrame `df`, specifying the column to be linked (`link`). Calling `show()` displays the resulting DataFrame:

+-----------+-----------------------------------+------------------------------------------------------------------------------+---------------------------------------------------------------------------+
| country   | link                              | link_in_boolean_http://dbpedia.org/resource/Category:Former_Spanish_colonies | link_in_boolean_http://dbpedia.org/resource/Category:East_Asian_countries |
+===========+===================================+==============================================================================+===========================================================================+
| Spain     | http://dbpedia.org/resource/Spain |                                                                        False |                                                                     False |
+-----------+-----------------------------------+------------------------------------------------------------------------------+---------------------------------------------------------------------------+
| Japan     | http://dbpedia.org/resource/Japan |                                                                        False |                                                                      True |
+-----------+-----------------------------------+------------------------------------------------------------------------------+---------------------------------------------------------------------------+
| Chile     | http://dbpedia.org/resource/Chile |                                                                         True |                                                                     False |
+-----------+-----------------------------------+------------------------------------------------------------------------------+---------------------------------------------------------------------------+

