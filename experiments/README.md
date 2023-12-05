SparkKG-ML Evaluation
====================================

Experiments
============

Multiple factors are explored, including processing power, Spark cluster setup, SPARQL complexity, dataset dimensions, and result set scale, to examine their impact on the overall runtime performance. This examination is conducted in contrast to the DistRDF2ML framework. Furthermore, the performance of the additional features in our library is scrutinized, considering both runtime efficiency and overall performance metrics.

Setup
=======

Databricks Community Edition with 15.3 GB Memory and 2 Cores was employed for experiments involving comparisons with DistRDF2ML. In the experiments assessing only SparkKG-ML performance, Oracle Cloud Infrastructure and its Data Science and Data Flow services were utilized, offering up to 64 cores per executor and up to 512 GB of memory in total. The Databricks configuration employed Python 3.9, PySpark 3.5, and Scala 2.12, while the Oracle Cloud setting utilized Python 3.8 and PySpark 3.2.

Datesets
=========

Two real–world datasets were used: 
1) Linked Movie Database (LMDB): An openly accessible knowledge graph of facts about 38,000 movies (including title, runtime, actors, genres, producers, and country of origin).
2) IMDb Non–Commercial Dataset: Comprises facts (i.e., title, year, runtime, and genres) for 10, 263, 447 movies.

Additionally a synthetic datasets was used, initially created for the evaluation of DistRDF2ML. Specifically, the number of movies is exponentially increased, while the resultset size and feature density remain unchanged.

Files
=====

**Functionality:**  The file comprises experiments for binary and multiclass classification conducted with SparkKG-ML and DistRDF2ML. Additionally, it includes supplementary experiments exploring data augmentation and feature selection using the SparkKG-ML library.

**Performance:** This file includes source codes for assessing and comparing the performance of SparkKG-ML and DistRDF2ML. It explores various factors like processing power, Spark cluster configuration, SPARQL complexity, dataset dimensions, and result set scale, examining how these factors influence the overall runtime performance.

**Pipelines:** In this file, three pipelines are presented to compare the lines of code needed to achieve the same end goal – training a machine learning model with semantic data. The comparison is made between direct implementation in Python (without SparkKG-ML) and implementation in Scala using the SANSA Stack with the DistRDF2ML library.
