SERE Evaluation
====================================

Experiments
============

We demonstrate the limitations of existing approaches and the advantages of our proposed method using a collection of datasets. Additionally, we investigate the impact of different Spark configurations on SERE's execution time. Finally, we perform a classification task to demonstrate the competitiveness of SERE in terms of accuracy and scalability.

Setup
=======

For comparisons with pyRDF2Vec, we employed the Oracle Cloud Infrastructure (OCI) and its Data Science service. Specifically, we used a configuration with 4 CPUs and 16 GB of memory, representing a typical commodity machine setup. To examine the impact of different Spark configurations on the runtime of SERE, we utilized OCI's Data Flow service, providing up to 64 cores per executor and up to 256 GB of memory. All experiments were conducted using Python 3.8, PySpark 3.2, and pyRDF2Vec 0.2.3. The experiments involving different Spark setups were conducted three times, and the average performance is reported.

Datesets
=========

1. **[MUTAG](https://www.uni-mannheim.de/dws/research/resources/sw4ml-benchmark/):** Contains information about complex molecules, specifically focusing on their potential carcinogenicity, indicated by the isMutagenic property.
2. **[AM](https://www.uni-mannheim.de/dws/research/resources/sw4ml-benchmark/):** Records information about artifacts in the Amsterdam Museum.
3. **[RecipeKG](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/99PNJ5):** Contains facts about recipes and their ingredients, the cuisine they belong to, and nutritional information.


Files
=====

- **pyRDF2Vec_AM_path_num.ipynb:** Demonstrates path-based computation using the pyRDF2Vec approach on the AM dataset, focusing on the influence of the `path_num` parameter on embeddings generation.
- **pyRDF2vec_MUTAG_classification.ipynb:** Shows how the pyRDF2Vec framework can be applied to the MUTAG dataset for classification tasks.
- **pyRDF2Vec_recipeKG_path_num.ipynb:** Applies the pyRDF2Vec approach to the RecipeKG dataset, emphasizing the effect of varying the `path_num` parameter in the embedding generation process.
- **SERE_AM_executor#.ipynb:** Analyzes the impact of different executor settings within the SERE framework on the AM dataset.
- **SERE_AM_executor#+partition#.ipynb:** Combines the analysis of executor settings and partitioning strategies within the SERE framework for the AM dataset to understand their combined effect on performance.
- **SERE_AM_partition#.ipynb:** Focuses on partitioning strategies applied to the AM dataset within the SERE framework and their impact on runtime and performance.
- **SERE_AM_path_num.ipynb:** Explores the effect of the `path_num` parameter within the SERE framework on the AM dataset.
- **SERE_MUTAG_classification.ipynb:** Similar to the pyRDF2Vec MUTAG notebook, but applies the SERE framework to perform classification tasks on the MUTAG dataset.
- **SERE_recipeKG_path_num.ipynb:** Investigates the impact of the `path_num` parameter on the RecipeKG dataset when using the SERE framework for embedding generation.