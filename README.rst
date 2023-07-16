Welcome to the SparkKG-ML Documentation
====================================

Welcome to the documentation for **sparkKG-ML**, a Python library designed to facilitate machine learning with Spark on semantic web and knowledge graph data. 

SparkKG-ML is specifically built to bridge the gap between the semantic web data model and the powerful distributed computing capabilities of Apache Spark. By leveraging the flexibility of semantic web and the scalability of Spark, sparkKG-ML empowers you to extract meaningful insights and build robust machine learning models on semantic web and knowledge graph datasets.

You can find the detailed documentaion of sparkKG-ML `here <https://sparkkgml.readthedocs.io/en/latest/index.html#>`_. This documentation serves as a comprehensive guide to understanding and effectively utilizing sparkKG-ML. Here, you will find detailed explanations of the library's core concepts, step-by-step tutorials to get you started, and a rich collection of code examples to illustrate various use cases.

**Key Features of sparkKG-ML:**

1. **Seamless Integration:** SparkKG-ML seamlessly integrates with Apache Spark, providing a unified and efficient platform for distributed machine learning on semantic web and knowledge graph data.

2. **Data Processing:** With SparkKG-ML, you can easily preprocess semantic web data, handle missing values, perform feature engineering, and transform your data into a format suitable for machine learning.

3. **Scalable Machine Learning:** SparkKG-ML leverages the distributed computing capabilities of Spark to enable scalable and parallel machine learning on large semantic web and knowledge graph datasets.

4. **Advanced Algorithms:** SparkKG-ML provides a wide range of machine learning algorithms specifically designed for semantic web and knowledge graph data, allowing you to tackle complex tasks within the context of knowledge graphs and the semantic web.

5. **Extensibility:** SparkKG-ML is designed to be easily extended, allowing you to incorporate your own custom algorithms and techniques seamlessly into the library.

We hope this documentation proves to be a valuable resource as you explore the capabilities of sparkKG-ML and embark on your journey of machine learning with Spark on semantic web and knowledge graph data. Happy learning!


Installation Guide
==================

This guide provides step-by-step instructions on how to install the sparkKG-ML library. sparkKG-ML can be installed using `pip` or by installing from the source code.

**Installation via pip:**

To install sparkKG-ML using `pip`, follow these steps:

1. Open a terminal or command prompt.

2. Run the following command to install the latest stable version of sparkKG-ML:

   .. code-block:: bash
      
      pip install sparkkgml

   This will download and install sparkKG-ML and its dependencies.

3. Once the installation is complete, you can import sparkKG-ML into your Python projects and start using it for machine learning on semantic web and knowledge graph data.

**Installation from source:**

To install sparkKG-ML from the source code, follow these steps:

1. Clone the sparkKG-ML repository from GitHub using the following command:

   .. code-block:: bash

      git clone https://github.com/bedirhangergin/sparkkgml.git

   This will create a local copy of the sparkKG-ML source code on your machine.

2. Change into the sparkKG-ML directory:

   .. code-block:: bash

      cd sparkkgml

3. Run the following command to install sparkKG-ML and its dependencies:

   .. code-block:: bash

      pip install .

   This will install sparkKG-ML using the source code in the current directory.

4. Once the installation is complete, you can import sparkKG-ML into your Python projects and start using it for machine learning on semantic web and knowledge graph data.

Congratulations! You have successfully installed the sparkKG-ML library. You are now ready to explore the capabilities of sparkKG-ML and leverage its machine learning functionalities.

For more details on how to use sparkKG-ML, please refer to the `documentation <https://sparkkgml.readthedocs.io/en/latest/index.html#>`_.


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

For more details on using sparkKG-ML and its functions, please refer to the `documentation <https://sparkKGML.readthedocs.io/en/latest/index.html#>`_.

License
=========

sparkKG-ML was created by `IDIAS Lab <http://www.cs.albany.edu/~cchelmis/ideaslab.html>`_. It is licensed under the terms of the Apache License 2.0 license.


