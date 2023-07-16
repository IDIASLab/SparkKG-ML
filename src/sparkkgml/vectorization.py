#!/usr/bin/env python
# coding: utf-8

# In[31]:


#import findspark
#findspark.find()
#findspark.init()


# In[32]:


from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer,Tokenizer,StopWordsRemover,Word2Vec,VectorAssembler
from pyspark.sql.types import StringType,IntegerType,FloatType,DoubleType,LongType,BooleanType
#from pyspark.sql.functions import collect_list,size,col,explode_outer
from pyspark.sql.functions import *


# In[33]:


class Vectorization:
    
    """
    A class for data preprocessing tasks such as null handling, feature extraction, and vectorization.

    Attributes:
        _entityColumn (str): The name of the entity column.
        _labelColumn (str): The name of the label column.
        _features (list): A list of features containing information about each column.
        _stopWordsRemover (bool): A flag indicating whether to remove stop words during word embedding.
        _word2vecSize (int): The size of the word vectors in Word2Vec embedding.
        _word2vecMinCount (int): The minimum count of words required for Word2Vec embedding.
        _digitStringStrategy (str): The strategy for digitizing string values ('index' or 'hash').

    """

    global spark
    spark = SparkSession.builder.getOrCreate()
     
    def __init__(self):
        self._entityColumn = ''
        self._stopWordsRemover=True        
        self._word2vecSize=2
        self._word2vecMinCount=1
        self._digitStringStrategy = "index" # index | hash
        
    
    # getter functions
    
    def get_entityColumn(self):
        return self._entityColumn
    
    def get_word2vecSize(self):
        return self._word2vecSize
    
    def get_stopWordsRemover(self):
        return self._StopWordsRemover
    
    def get_word2vecMinCount(self):
        return self._word2vecMinCount
    
    def get_digitStringStrategy(self):
        return self._digitStringStrategy
    
    
    # setter functions
    
    def set_entityColumn(self, entityColumn):
        self._entityColumn= entityColumn
        
    def set_word2vecSize(self, word2vecSize):
        self._word2vecSize= word2vecSize
        
    def set_word2vecMinCount(self, word2vecMinCount):
        self._word2vecMinCount= word2vecMinCount
        
    def set_stopWordsRemover(self, StopWordsRemover):
        self._StopWordsRemover= StopWordsRemover
        
    def set_digitStringStrategy(self, digitStringStrategy):
        self._digitStringStrategy= digitStringStrategy
        
    
    
    def vectorize(self, df2, features):
        """
        Vectorizes the specified columns in the DataFrame based on the provided features.

        Args:
            df2 (pyspark.sql.DataFrame): The input DataFrame.
            features (dict): A dictionary containing information about the features of each column.

        Returns:
            fullDigitizedDf (pyspark.sql.DataFrame): The vectorized DataFrame.

        Raises:
            NotImplementedError: If a transformation for a specific data type is not implemented.

        Implementation Flow:
            1. Iterate over each column in the DataFrame.

            2. Check the data type and features of the column to determine the vectorization strategy.

            3. Apply the appropriate transformation based on the column's data type and features.

               - If the column is a Single Categorical String:
                   - Apply string indexing or hashing based on the configured strategy.

               - If the column is a List of Categorical Strings:
                   - Explode the list and apply string indexing or hashing based on the configured strategy.

               - If the column is a Single Non-Categorical String:
                   - Apply Word2Vec embedding after tokenization and optional stop word removal.

               - If the column is a List of Non-Categorical Strings:
                   - Combine the list elements, apply tokenization, optional stop word removal, and Word2Vec embedding.

               - If the column is a Numeric type (Integer, Long, Float, Double):
                   - Handle both Single and List types by either joining or exploding the values.

               - If the column is a Boolean type:
                   - Cast the Boolean values to Integer (0 or 1).

               - If the column is of an unsupported data type:
                   - Raise a NotImplementedError.

            4. Join the transformed column with the vectorized DataFrame using the entity column.

            5. Return the resulting vectorized DataFrame.

        Note:
            The implementation follows a conditional branching based on the data type and features of each column
            to determine the appropriate vectorization strategy.

        """
        #if entity column is not set, set it as the first column
        if self._entityColumn == '':
            self._entityColumn=df2.columns[0]
            print(f'No entity column has been set, that is why the first column {self._entityColumn} is used as entity column')
            
        fullDigitizedDf=df2.select(self._entityColumn)
        
        for columnName in [col for col in df2.columns if col != self._entityColumn]:
    
            if features[columnName]['datatype']==StringType():

                #Single_Categorical_String
                if features[columnName]['isListOfEntries']==False and features[columnName]['isCategorical']==True:
                    
                    if self._digitStringStrategy == "index":

                        #newFeatureColumnName = columnName + "(IndexedString)"

                        indexer = StringIndexer().setInputCol(columnName).setOutputCol("output")
                        digitizedDf = indexer.fit(df2).transform(df2).select(self._entityColumn, "output")

                        fullDigitizedDf = fullDigitizedDf.join(digitizedDf,self._entityColumn).withColumnRenamed("output", columnName)
                        
                    elif self._digitStringStrategy == "hash":
                        
                        hashedDf = df2.withColumn("output", hash(col(columnName)).cast("double")) \
                                       .select(self._entityColumn, "output")
        

                        fullDigitizedDf = fullDigitizedDf.join(hashedDf,self._entityColumn).withColumnRenamed("output", columnName)

                #ListOf_Categorical_String
                elif features[columnName]['isListOfEntries']==True and features[columnName]['isCategorical']==True:

                    #newFeatureColumnName = columnName + "(ListOfIndexedString)"

                    #transform list to single  by exploding
                    inputDf = df2.select(col(self._entityColumn), explode_outer(col(columnName)))
                    
                    if self._digitStringStrategy == "index":

                        indexer = StringIndexer().setInputCol("col").setOutputCol("outputTmp")

                        digitizedDf = indexer.fit(inputDf).transform(inputDf) \
                                .select(self._entityColumn, "outputTmp") \

                        '''
                        digitizedDf = indexer.fit(inputDf).transform(inputDf) \
                                .groupBy(_entityColumn) \
                                .agg(collect_list("outputTmp").alias("output")) \
                                .select(_entityColumn, "output") \
                                .withColumnRenamed("output", newFeatureColumnName) \
                                .select(_entityColumn, newFeatureColumnName)
                        '''

                        fullDigitizedDf = fullDigitizedDf.join(digitizedDf,self._entityColumn).withColumnRenamed("outputTmp", columnName)
                
                    elif self._digitStringStrategy == "hash":
                        
                        hashedDf = inputDf.withColumn("output", hash(col("col")).cast("double")) \
                                       .select(self._entityColumn, "output")
                
                        fullDigitizedDf = fullDigitizedDf.join(hashedDf,self._entityColumn).withColumnRenamed("output", columnName)
                
                #Single_NonCategorical_String
                elif features[columnName]['isListOfEntries']==False and features[columnName]['isCategorical']==False:

                    #newFeatureColumnName = columnName +"(Word2Vec)"
                    
                    tokenizer = Tokenizer(inputCol=columnName, outputCol="words")
                    df2 = tokenizer.transform(df2)
                    tempColumnName="words"
                    
                    if self._stopWordsRemover==True:

                        remover = StopWordsRemover(inputCol="words", outputCol="filtered")
                        df2 = remover.transform(df2)
                        tempColumnName='filtered'

                    word2vec = Word2Vec(inputCol=tempColumnName, outputCol="output", minCount=self._word2vecMinCount, vectorSize=self._word2vecSize)
                    word2vecModel = word2vec.fit(df2)
                    digitizedDf=word2vecModel.transform(df2) \
                                             .select(self._entityColumn, "output")

                    fullDigitizedDf = fullDigitizedDf.join(digitizedDf,self._entityColumn).withColumnRenamed("output", columnName)


                #ListOf_NonCategorical_String
                elif features[columnName]['isListOfEntries']==True and features[columnName]['isCategorical']==False:

                    #newFeatureColumnName = columnName +"(Word2Vec)"

                    dfCollapsedTwoColumnsNullsReplaced = df2 \
                        .withColumn("sentences", concat_ws(". ", col(columnName))) \
                        .select(self._entityColumn, "sentences")
                        
                    tokenizer = Tokenizer().setInputCol("sentences").setOutputCol("words")
                    tokenizedDf = tokenizer.transform(dfCollapsedTwoColumnsNullsReplaced).select(self._entityColumn, "words")
                    tempColumnName='words'    
                        
                    if self._stopWordsRemover==True:

                        remover = StopWordsRemover().setInputCol("words").setOutputCol("filtered")
                        tokenizedDf = remover.transform(tokenizedDf).select(self._entityColumn, "filtered")
                        tempColumnName='filtered'
                        
                    word2vec = Word2Vec(inputCol=tempColumnName, outputCol="output", minCount=self._word2vecMinCount, vectorSize=self._word2vecSize)    
                    word2vecModel = word2vec.fit(tokenizedDf)
                    digitizedDf=word2vecModel.transform(tokenizedDf)\
                                            .select(self._entityColumn, "output")
                        
                    fullDigitizedDf = fullDigitizedDf.join(digitizedDf,self._entityColumn).withColumnRenamed("output", columnName)


            elif features[columnName]['datatype']==IntegerType() or features[columnName]['datatype']==LongType() or \
                 features[columnName]['datatype']==FloatType() or features[columnName]['datatype']==DoubleType():

                 #if list, need to convert to single value
                if features[columnName]['isListOfEntries']==True:
                        digitizedDf=df2.select(self._entityColumn,explode_outer(columnName).alias(columnName))
                        fullDigitizedDf = fullDigitizedDf.join(digitizedDf,self._entityColumn)  

                #if single, add to df
                else:
                    digitizedDf=df2.select(self._entityColumn,columnName)
                    fullDigitizedDf = fullDigitizedDf.join(digitizedDf,self._entityColumn)

            elif features[columnName]['datatype']==BooleanType():
                
                digitizedDf=df2.select(self._entityColumn,columnName)
                #casting Boolean Type to Integer where it converts values to 0's and 1's
                digitizedDf=digitizedDf.withColumn(columnName,col(columnName).cast(IntegerType()))
                
                fullDigitizedDf = fullDigitizedDf.join(digitizedDf,self._entityColumn)
                
            #if it is not String     
            else:
                type1=features[columnName]['datatype']
                print( f'Column {columnName} can not be added. Transformation for type {type1} not implemented yet.')
              
        
        
        return fullDigitizedDf

