#!/usr/bin/env python
# coding: utf-8

# In[18]:


#import findspark
#findspark.find()
#findspark.init()
#from pyspark.sql import SparkSession


# In[ ]:


from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
import numpy as np


# In[19]:


def correlation_feature_selection(df, threshold, features_arr):
    
    """
        Perform correlation-based feature selection on a DataFrame.

        Args:
            df (pyspark.sql.DataFrame): The input DataFrame.
            threshold (float): The correlation threshold.
            features_arr (list[str]): A list of feature column names.

        Returns:
            pyspark.sql.DataFrame: The resulting DataFrame with selected non-correlated features.

        Notes:
            This function performs feature selection based on correlation. It follows these steps:

            1. Assemble the specified feature columns into a vector column.

            2. Compute the correlation matrix of the assembled features.

            3. Identify features that have low absolute correlation with all other features.

            4. Select the identified non-correlated features.

            5. Return the input DataFrame with only the selected non-correlated feature columns.
    """
    
    #spark = SparkSession.builder.getOrCreate()
    
    assembler = VectorAssembler(inputCols=features_arr, outputCol="features")
    df_assembled = assembler.transform(df).select("features")
    
    corr_mat = (Correlation.corr(df_assembled, 'features').collect()[0][0]).toArray()
    new_columns=[]
    
    for i in range(len(features_arr)):
        flag=False
        for j in range(i+1, len(features_arr)):
            #check if it has high corr with any other column
            if np.abs(corr_mat[i][j]) > threshold:
                flag=True
        #add to new column list if it has no high corr with any other column        
        if flag==False:
            new_columns.append(features_arr[i])
    
    return df.select(new_columns)

