#!/usr/bin/env python
# coding: utf-8

# In[17]:


#import findspark
#findspark.find()
#findspark.init()


# In[7]:


import pandas as pd
import re
from sparkkgml.data_acquisition import DataAcquisition
from kgextension.endpoints import DBpedia
from pyspark.sql import SparkSession


# In[8]:


def clean_column_names(df):
    
    """
        Clean column names of a Pandas DataFrame by removing invalid characters.

        Args:
            df (pandas.DataFrame): The input Pandas DataFrame.

        Returns:
            pandas.DataFrame: The DataFrame with cleaned column names.
    """
    
    # Regular expression pattern for removing invalid characters
    pattern = r'[\s`/\\,:;\'"\[\]\{\}\(\).]'
    
    # Apply strip to remove leading and trailing whitespace characters
    #cleaned_columns = [col.strip() for col in df]
    
    # Clean the column names
    cleaned_columns = [re.sub(pattern, '', col) for col in df]
    
    # Create a dictionary to map original column names to cleaned column names
    column_mapping = {col: cleaned_col for col, cleaned_col in zip(df.columns, cleaned_columns)}
    
    #Rename the columns in the DataFrame
    df_cleaned = df.rename(columns=column_mapping)
    
    return df_cleaned


# In[9]:


def spark_dbpedia_lookup_linker(sparkDataFrame, column, new_attribute_name='new_link', progress=True, 
                                base_url='https://lookup.dbpedia.org/api/search/', max_hits=1, query_class='', 
                                lookup_api='KeywordSearch', caching=True):
    
    """
        Perform DBpedia entity linking on a Spark DataFrame using the DBpedia Spotlight service.

        Args:
            sparkDataFrame (pyspark.sql.DataFrame): The input Spark DataFrame.
            column (str): Name of the column whose entities should be found.
            new_attribute_name (str, optional): Name of the column containing the link to the knowledge graph.
                                                Defaults to 'new_link'.
            progress (bool, optional): If True, progress bars will be shown to inform the user about the progress.
                                       Defaults to True.
            base_url (str, optional): The base URL of the DBpedia Lookup API. Defaults to 'https://lookup.dbpedia.org/api/search/'.
            max_hits (int, optional): Maximal number of URIs that should be returned per entity. Defaults to 1.
            query_class (str, optional): Specifies whether the entities that occur first ('first'), that have the highest support
                                         ('support'), or that have the highest similarity score ('similarityScore') should be chosen.
                                         Defaults to ''.
            lookup_api (str, optional): The DBpedia Lookup API to use. Defaults to 'KeywordSearch'.
            caching (bool, optional): Turn result-caching for queries issued during the execution on or off. Defaults to True.

        Returns:
            pyspark.sql.DataFrame: DataFrame with new column(s) containing the DBpedia URIs.

        Notes:
            This function performs DBpedia entity linking on a Spark DataFrame using the DBpedia Spotlight service. It follows these steps:

            1. Convert the Spark DataFrame to a Pandas DataFrame.

            2. Apply the `dbpedia_lookup_linker` function from the kgextension library to the Pandas DataFrame.

            3. Convert the resulting Pandas DataFrame back to a Spark DataFrame.

            4. Return the Spark DataFrame with the new column(s) containing the DBpedia URIs.
        """
    from kgextension.linking import dbpedia_lookup_linker
    
    spark = SparkSession.builder.getOrCreate()
    
    DataAcquisitionObject=DataAcquisition()
    
    pandasDf=dbpedia_lookup_linker(sparkDataFrame.toPandas(), column, new_attribute_name, progress, base_url, max_hits, query_class, lookup_api, caching)
    
    sparkDataFrame2= spark.createDataFrame(clean_column_names(DataAcquisitionObject.nullDrop(pandasDf)))
    
    return sparkDataFrame2


# In[10]:


def spark_specific_relation_generator(sparkDataFrame, columns, endpoint=DBpedia, uri_data_model=False, progress=True, 
                                      direct_relation="http://purl.org/dc/terms/subject", 
                                      hierarchy_relation=None, max_hierarchy_depth=1, prefix_lookup=False, caching=True):
    
    
    """
        Generate attributes from a specific direct relation on a Spark DataFrame.

        Args:
            sparkDataFrame (pyspark.sql.DataFrame): The input Spark DataFrame.
            columns (str or list): Name(s) of column(s) that contain(s) the link(s) to the knowledge graph.
            endpoint (Endpoint, optional): SPARQL Endpoint to be queried; ignored when 'uri_data_model' is True.
                                           Defaults to DBpedia.
            uri_data_model (bool, optional): If enabled, the URI is directly queried instead of a SPARQL endpoint.
                                             Defaults to False.
            progress (bool, optional): If True, progress bars will be shown to inform the user about the progress made
                                       by the process. Defaults to True.
            direct_relation (str, optional): Direct relation used to create features. Defaults to
                                             'http://purl.org/dc/terms/subject'.
            hierarchy_relation (str, optional): Hierarchy relation used to connect categories. Defaults to None.
            max_hierarchy_depth (int, optional): Maximal number of hierarchy steps taken. Defaults to 1.
            prefix_lookup (bool/str/dict, optional): True: Namespaces of prefixes will be looked up at prefix.cc and added
                                                     to the SPARQL query.
                                                     str: User provides the path to a JSON file with prefixes and namespaces.
                                                     dict: User provides a dictionary with prefixes and namespaces.
                                                     Defaults to False.
            caching (bool, optional): Turn result-caching for queries issued during the execution on or off. Defaults to True.

        Returns:
            pyspark.sql.DataFrame: DataFrame with additional features.

        Notes:
            This function generates attributes from a specific direct relation on a Spark DataFrame. It follows these steps:

            1. Convert the Spark DataFrame to a Pandas DataFrame.

            2. Apply the `specific_relation_generator` function from the kgextension library to the Pandas DataFrame.

            3. Convert the resulting Pandas DataFrame back to a Spark DataFrame.

            4. Return the Spark DataFrame with the additional features.
            
    """
    from kgextension.generator import specific_relation_generator
    
    spark = SparkSession.builder.getOrCreate()
    
    DataAcquisitionObject=DataAcquisition()

    pandasDf=specific_relation_generator(sparkDataFrame.toPandas(), columns, endpoint, uri_data_model, progress, direct_relation, hierarchy_relation, max_hierarchy_depth, prefix_lookup, caching)
    
    sparkDataFrame2= spark.createDataFrame(clean_column_names(DataAcquisitionObject.nullDrop(pandasDf)))
    
    return sparkDataFrame2


# In[ ]:




