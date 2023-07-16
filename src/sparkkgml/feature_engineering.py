#!/usr/bin/env python
# coding: utf-8

# In[81]:


#import findspark
#findspark.find()
#findspark.init()


# In[ ]:


from pyspark.sql import SparkSession
#from pyspark.sql.functions import collect_list,size,col
from pyspark.sql.functions import *


# In[88]:


class FeatureEngineering:
    
    """
    A class for feature collection.

    Attributes:
       
        _entityColumn (str): The name of the entity column.
            

    """

    global spark
    spark = SparkSession.builder.getOrCreate()
     
    def __init__(self):
        
        self._entityColumn = ''
        
    
    # getter functions

    def get_entityColumn(self):
        return self._entityColumn
    
    
    # setter functions

    def set_entityColumn(self, entityColumn):
        self._entityColumn= entityColumn
        
  
    def getFeatures(self, df):
        """Extracts features and their descriptions from a DataFrame.

        Args:
            df (pyspark.sql.DataFrame): The input DataFrame.

        Returns:
            tuple: A tuple containing the collapsed DataFrame and a dictionary of feature descriptions.

        Notes:
            This function analyzes each column in the input DataFrame and extracts features. The resulting features are
            stored in a collapsed DataFrame where each row represents a unique entity. A dictionary of feature descriptions
            provides information about each feature's properties.

        Feature Descriptions:

        - featureType (str): The type of the feature, combining information about whether it is a list or a single value, whether it is categorical or non-categorical, and the data type.

        - name (str): The name of the feature column.

        - nullable (bool): A flag indicating if the feature can have null values. Extracted based on the rule that a feature is nullable if it has at least one null value.

        - datatype (spark.DataType): The data type of the feature column.

        - numberDistinctValues (int): The number of distinct values in the feature column.

        - isListOfEntries (bool): A flag indicating if the feature is a list of entries. Extracted based on the rule that a feature is considered a list if it has more than one entry in at least one row.

        - isCategorical (bool): A flag indicating if the feature is categorical. Extracted based on the rule that a feature is considered categorical if the ratio of distinct values to the total number of entities is less than 0.1.

        """
        
        #if entity column is not set, set it as the first column
        if self._entityColumn == '':
            self._entityColumn=df.columns[0]
            print(f'No entity column has been set, that is why the first column {self._entityColumn} is used as entity column')
        
        keyColumnNameString=self._entityColumn
        featureColumns = [col for col in df.columns if col != keyColumnNameString]
        
        collapsedDataframe = df.select(keyColumnNameString).dropDuplicates()
        numberRows = collapsedDataframe.count()
        #dictionary to map all the features of columns
        featureDescriptions = {}
        
        #apply and get all the features for all the columns
        for currentFeatureColumnNameString in featureColumns:
            
            #two column df for key and selected feature
            twoColumnDf = df.select(keyColumnNameString, currentFeatureColumnNameString).dropDuplicates()
            #groupBy the column according to key 
            groupedTwoColumnDf = twoColumnDf.groupBy(keyColumnNameString)
            #make a list of feature if it is a list, add a new column for length of the list
            collapsedTwoColumnDfwithSize=groupedTwoColumnDf\
                                                    .agg(collect_list(currentFeatureColumnNameString).alias(currentFeatureColumnNameString))\
                                                    .withColumn("size", size(col(currentFeatureColumnNameString)))
            
            minNumberOfElements = collapsedTwoColumnDfwithSize.select("size").agg(min("size")).first()[0]
            maxNumberOfElements = collapsedTwoColumnDfwithSize.select("size").agg(max("size")).first()[0]
            
            #determine the feature specific properties
            nullable = True if minNumberOfElements == 0 else False
            datatype = twoColumnDf.schema[currentFeatureColumnNameString].dataType 
            numberDistinctValues = twoColumnDf.select(currentFeatureColumnNameString).distinct().count()
            isListOfEntries = True if maxNumberOfElements > 1 else False
            #availability = collapsedTwoColumnDfwithSize.filter(F.col("size") > 0).count() / numberRows
            isCategorical = True if (numberDistinctValues / numberRows) < 0.1 else False
            
            #append a string for feature type "ListOf|Single + Categorical|NonCategorical + dataType"
            featureType = "ListOf_" if isListOfEntries else "Single_"
            featureType += "Categorical_" if isCategorical else "NonCategorical_"
            featureType += str(datatype).split("Type")[0]

            featureSummary = {
                    "featureType": featureType,
                    "name": currentFeatureColumnNameString,
                    "nullable": nullable,
                    "datatype": datatype,
                    "numberDistinctValues": numberDistinctValues,
                    "isListOfEntries": isListOfEntries,
                    "isCategorical": isCategorical,
                    #"avalability": availability
            }
            
            #append the features dictionary to mapping
            featureDescriptions[currentFeatureColumnNameString] = featureSummary
            
            #append the end-result feature column to end-result dataframe
            if isListOfEntries:
                joinable_df = collapsedTwoColumnDfwithSize.select(keyColumnNameString, currentFeatureColumnNameString)
            else:
                joinable_df = twoColumnDf.select(keyColumnNameString, currentFeatureColumnNameString)

            collapsedDataframe = collapsedDataframe.join(joinable_df, keyColumnNameString)
            
            
        return collapsedDataframe, featureDescriptions

