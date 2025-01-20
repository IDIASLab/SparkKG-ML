[![PyPI Downloads](https://static.pepy.tech/badge/sparkkgml)](https://pepy.tech/projects/sparkkgml)
![GitHub Stars](https://img.shields.io/github/stars/IDIASLab/SparkKG-ML?style=social)
![Python Versions](https://img.shields.io/pypi/pyversions/sparkkgml)

Welcome to the SparkKG-ML Documentation
====================================

Welcome to the documentation for **SparkKG-ML**, a Python library designed to facilitate machine learning with Spark on semantic web and knowledge graph data. 

SparkKG-ML is specifically built to bridge the gap between the semantic web data model and the powerful distributed computing capabilities of Apache Spark. By leveraging the flexibility of semantic web and the scalability of Spark, SparkKG-ML empowers you to extract meaningful insights and build robust machine learning models on semantic web and knowledge graph datasets.

You can find the detailed documentaion of SparkKG-ML [here](https://sparkkgml.readthedocs.io/en/latest/index.html#). This documentation serves as a comprehensive guide to understanding and effectively utilizing SparkKG-ML. Here, you will find detailed explanations of the library's core concepts, step-by-step tutorials to get you started, and a rich collection of code examples to illustrate various use cases. 

Now you can also find  ``SERE`` ( **S**calable and Distributed Framework for Unsupervised 
**E**mbeddings Computation on La**R** ge-scale Knowl**E**dge Graphs) in our [documentation](https://sparkkgml.readthedocs.io/en/latest/sere.html). 


**Key Features of SparkKG-ML:**

1. **Seamless Integration:** SparkKG-ML seamlessly integrates with Apache Spark, providing a unified and efficient platform for distributed machine learning on semantic web and knowledge graph data.

2. **Data Processing:** With SparkKG-ML, you can easily preprocess semantic web data, handle missing values, perform feature engineering, and transform your data into a format suitable for machine learning.

3. **Scalable Machine Learning:** SparkKG-ML leverages the distributed computing capabilities of Spark to enable scalable and parallel machine learning on large semantic web and knowledge graph datasets.

4. **Advanced Algorithms:** SparkKG-ML provides a wide range of machine learning algorithms specifically designed for semantic web and knowledge graph data, allowing you to tackle complex tasks within the context of knowledge graphs and the semantic web.

5. **Extensibility:** SparkKG-ML is designed to be easily extended, allowing you to incorporate your own custom algorithms and techniques seamlessly into the library.

We hope this documentation proves to be a valuable resource as you explore the capabilities of SparkKG-ML and embark on your journey of machine learning with Spark on semantic web and knowledge graph data. Happy learning!

Installation Guide
==================

This guide provides step-by-step instructions on how to install the SparkKG-ML library. SparkKG-ML can be installed using `pip` or by installing from the source code.

**Installation via pip:**

To install SparkKG-ML using `pip`, follow these steps:

1. Open a terminal or command prompt.

2. Run the following command to install the latest stable version of SparkKG-ML:

   ```bash
      
      pip install sparkkgml

This will download and install SparkKG-ML and its dependencies.

3. Once the installation is complete, you can import SparkKG-ML into your Python projects and start using it for machine learning on semantic web and knowledge graph data.

**Installation from source:**

To install SparkKG-ML from the source code, follow these steps:

1. Clone the SparkKG-ML repository from GitHub using the following command:

   ```bash

      git clone https://github.com/IDIASLab/SparkKG-ML

This will create a local copy of the SparkKG-ML source code on your machine.

2. Change into the SparkKG-ML directory:

   ```bash

      cd sparkkgml

3. Run the following command to install SparkKG-ML and its dependencies:

   ```bash

      pip install .

This will install SparkKG-ML using the source code in the current directory.

4. Once the installation is complete, you can import SparkKG-ML into your Python projects and start using it for machine learning on semantic web and knowledge graph data.

Congratulations! You have successfully installed the SparkKG-ML library. You are now ready to explore the capabilities of SparkKG-ML and leverage its machine learning functionalities.

For more details on how to use SparkKG-ML, please refer to the [documentation](https://sparkkgml.readthedocs.io/en/latest/index.html#).

## Getting Started

Let's start with a basic example, we will retrieve data from a SPARQL endpoint and convert it into a Spark DataFrame using the `getDataFrame` function.

```python
        # Import the required libraries
        from sparkkgml.data_acquisition import DataAcquisition
        
        # Create an instance of DataAcquisition 
        DataAcquisitionObject = DataAcquisition()

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
```

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

SERE
============

``SERE`` is a scalable and distributed embedding framework designed for large-scale KGs (KGs), leveraging the distributed computing capabilities of Apache Spark. The framework enables the extraction of walks over a KG and then creates embeddings from these walks, fully implemented in Spark, making them ready for integration into Machine Learning (ML) pipelines.

KGs store RDF data in a graph format, where entities are linked by relations. To compute RDF data embeddings, the graph representation is converted into sequences of entities. These sequences are processed by neural language models, such as Word2Vec, treating them like sentences composed of words. This allows the model to represent each entity in the RDF graph as a vector of numerical values in a latent feature space.

``SERE`` allows the computation of embeddings over very large KGs in scenarios where such embeddings were previously not feasible due to a significantly lower runtime and improved memory requirements. ``SERE`` is open-source, well-documented, and fully integrated into the SparkKG-ML Python library, which offers end-to-end ML pipelines over semantic data stored in KGs directly in Python.

Check out for more detailed [documentation](https://sparkkgml.readthedocs.io/en/latest/sere.html).

Evaluation
==========

To evaluate SparkKG-ML, we investigated various factors to understand their impact on overall runtime performance, comparing the results with an existing framework. The [experiments](https://github.com/IDIASLab/SparkKG-ML/tree/main/experiments) folder contains detailed information for those interested, covering not only the general analysis but also the performance of our library's additional features, including both runtime efficiency and overall performance metrics.

License
=========

SparkKG-ML was created by [IDIAS Lab](http://www.cs.albany.edu/~cchelmis/ideaslab.html). It is licensed under the terms of the Apache License 2.0 license.

Acknowledgement
=================

This material is based upon work supported by the National Science Foundation under Grants No. CRII:III-1850097 and ECCS-1737443. This work was supported in part by Oracle Cloud credits and related resources provided by the Oracle for Research program.

Referencing
=================

For referencing SparkKG-ML, please use the citation below. 

```bibtex
@InProceedings{sparkkgml,
  author    = {Gergin, Bedirhan and Chelmis, Charalampos},
  title     = {SparkKG-ML: A Library to Facilitate End-to-End Large-Scale Machine Learning Over Knowledge Graphs in Python},
  booktitle = {The Semantic Web -- ISWC 2024},
  year      = {2025},
  editor    = {Demartini, Gianluca and others},
  series    = {Lecture Notes in Computer Science},
  volume    = {15233},
  pages     = {3--19},
  publisher = {Springer, Cham},
  isbn      = {978-3-031-77847-6},
  doi       = {10.1007/978-3-031-77847-6_1},
  url       = {https://doi.org/10.1007/978-3-031-77847-6_1}
}
```

For SERE, please use the citation below. 

```bibtex
@INPROCEEDINGS{SERE,
  author={Gergin, Bedirhan and Chelmis, Charalampos},
  booktitle={2024 IEEE International Conference on Big Data (BigData)}, 
  title={Large–Scale Knowledge Graph Embeddings in Apache Spark}, 
  year={2024},
  pages={243-251},
  doi={10.1109/BigData62323.2024.10825006}
}
```
