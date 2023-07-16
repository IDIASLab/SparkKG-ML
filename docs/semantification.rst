.. _semantification:

Semantification
==================

The Semantification module in RDFX provides functionality to transform machine learning results into RDF data. It plays a crucial role in completing the end-to-end process of a machine learning workflow and is an essential step in the RDFX library.

Machine learning models often produce classification results containing predictions, associated data, model names, and other relevant information. The Semantification module takes these machine learning results and generates a Turtle file based on a configured ontology. This process enhances the interpretability, integration, and sharing of the machine learning results by representing them in a standardized RDF format.

The resulting Turtle file can be easily queried, visualized, and integrated with other RDF datasets and semantic tools. It enables seamless interoperability and facilitates the integration of machine learning outputs into broader knowledge graphs and semantic ecosystems.

Example Usage
--------------

To illustrate the usage of the Semantification module, consider the following code snippet:

.. code-block:: python

    #import the module
    from sparkkgml.semantification import Semantification

    #create Semantification instance
    semantificationObject=Semantification()

    # Perform a classification task and obtain the prediction result in a dataframe
    # Prepare the necessary information for semantification
    semantificationObject.semantify(df, uri1='uri', label1='label', prediction1='prediction', dest='experiment.ttl')

In the example above, we assume that we first obtain the prediction result of a classification task using a trained machine learning model, stored in the dataFrame ``df``. 

Next, we use the ``semantify`` function to generate RDF data based on the configured ontology. The function maps the provided information to the ontology concepts and generates RDF triples accordingly.

Finally, we save the semantified data as a Turtle file, specifying the file name as "experiment.ttl". The resulting Turtle file can then be used for further analysis, integration with existing RDF datasets, or sharing with other systems that support RDF processing.

Please refer to the RDFX API documentation for more detailed information on the available functions, configuration options, and usage guidelines.

