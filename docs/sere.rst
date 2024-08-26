.. _sere:

Introduction
============

\ **S**\ calable and Distributed Framework for Unsupervised 
\ **E**\ mbeddings Computation on La\ **R**\ ge-scale Knowl\ **E**\ dge Graphs (``SERE``)

``SERE`` is a scalable and distributed embedding framework designed for large-scale KGs (KGs), leveraging the distributed computing capabilities of Apache Spark. The framework enables the extraction of walks over a KG and then creates embeddings from these walks, fully implemented in Spark, making them ready for integration into Machine Learning (ML) pipelines.

KGs store RDF data in a graph format, where entities are linked by relations. To compute RDF data embeddings, the graph representation is converted into sequences of entities. These sequences are processed by neural language models, such as Word2Vec, treating them like sentences composed of words. This allows the model to represent each entity in the RDF graph as a vector of numerical values in a latent feature space.

``SERE`` allows the computation of embeddings over very large KGs in scenarios where such embeddings were previously not feasible due to a significantly lower runtime and improved memory requirements. ``SERE`` is open-source, well-documented, and fully integrated into the SparkKG-ML Python library, which offers end-to-end ML pipelines over semantic data stored in KGs directly in Python.

Overview of SERE
---------------------

1. **KG Representation**:
   
   ``SERE`` provides a ``KG`` module with functionalities to manipulate and interact with RDF-based KGs using Apache Spark and GraphFrames. The KG class allows users to parse RDF data, create vertices and edges, and construct a GraphFrame for further analysis. There are two methods to acquire RDF data from a KG:
   
   - **Local Storage**: If the KG is stored locally as a flat file, provide the file path.
   - **Remote Access**: For remote KGs, provide a SPARQL Endpoint to access the data.
  
   After acquiring the RDF data, it is converted into a `GraphFrame`, enabling Spark-based distributed processing.

2. **Graph Walks**:
   
   The next step involves generating sequences for each entity in the KG. Each sequence starts with the entity and is followed by "words" representing relationships, other entities, or literals, reflecting the connections in the KG.
   
   ``SERE`` leverages the motif-finding function in `GraphFrames` to extract all possible walks of a given depth with ``MotifWalks`` module. Motifs describe structural patterns in the graph, which are used to generate sequences for embedding computation.
   
   After extracting patterns, sequences starting from specific entities of interest are retained. This filtering can simulate different walk strategies:
   
     - **BFS**: Breadth-first search up to a given depth from a root entity.
     - **Entity Walks**: Focuses on entities by excluding predicates.
     - **Property Walks**: Focuses on predicates by excluding entities.
   
   Users must specify entities for embedding, walk lengths, and any entities or predicates to exclude, which is especially crucial for preventing data leakage in ML processes.

4. **Embedding Computation**:
   
   Once the sequences are generated, a neural language model such as Word2Vec is trained. ``SERE`` uses the Skip-gram model from Spark's MLlib package, allowing users to tune hyperparameters for model customization.
   
   The resulting embeddings are stored as a Spark DataFrame, ready for use in downstream tasks like node classification.

Conclusion
----------

``SERE`` efficiently processes large-scale KGs by exploiting Spark's distributed computing capabilities. This framework enables the computation of embeddings in scenarios where it was previously infeasible due to large data volumes, reduced runtime, and improved memory requirements. The embeddings produced by ``SERE`` are fully integrated into the SparkKG-ML library, supporting end-to-end ML pipelines over semantic data stored in KGs.