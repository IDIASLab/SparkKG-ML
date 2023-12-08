import sparql_dataframe
import pandas as pd
from rdflib import Graph
from io import StringIO
import csv




class DataAcquisition:
    
    """
    A class for knowledge graph query and data preprocessing tasks such as null handling and null Drop.

    Attributes:
        _endpoint (str): The endpoint for retrieving data.
        _query (str): The query for retrieving data.
        _amputationMethod (str): The method for handling null values in the DataFrame ('nullReplacement' or 'nullDrop').
        _rowNullDropPercent (int): The percentage threshold for dropping rows with null values.
        _columnNullDropPercent (int): The percentage threshold for dropping columns with null values.
        _nullReplacementMethod (str): The method for replacing null values ('median', 'mean', 'mode', or 'customValue').
        _customValueVariable (str): The name of the variable used as a custom replacement value for null values.
        _customStringValueVariable (str): The name of the variable used as a custom replacement string for null values.

    """

    #sparkSession = None
    #spark = SparkSession.builder.getOrCreate()
     
    def __init__(self, spark_session: SparkSession = None):
        """
            Initializes the DataAcquisition class.

            Parameters:
                spark_session (SparkSession, optional): The Spark session for working with Spark DataFrame.
                    If not provided, a new session will be created using SparkSession.builder.getOrCreate().
        """
        if spark_session is None:
            # If no custom SparkSession is provided, create a new one.
            self.spark_session = SparkSession.builder.getOrCreate()
        else:
            # Use the provided SparkSession.
            self.spark_session = spark_session
   
        self._endpoint = ''
        self._query = ''
        
        self._handleNullValues='True'
        self._amputationMethod='nullDrop' # nullReplacement|nullDropp
        self._rowNullDropPercent=100 # it is percent %
        self._columnNullDropPercent=0 # it is percent %
        self._nullReplacementMethod='mod' # median|mean|mode|customValue
        self._customValueVariable=''
        self._customStringValueVariable=''
        
    
    # getter functions
    
    def get_endpoint(self):
        return self._endpoint
    
    def get_query(self):
        return self._query
    
    def get_amputationMethod(self):
        return self._amputationMethod
    
    def get_rowNullDropPercent(self):
        return self._rowNullDropPercent
    
    def get_columnNullDropPercent(self):
        return self._columnNullDropPercent
    
    def get_nullReplacementMethod(self):
        return self._nullReplacementMethod
    
    def get_customValueVariable(self):
        return self._customValueVariable
    
    def get_customStringValueVariable(self):
        return self._customStringValueVariable
    

    # setter functions
    
    def set_endpoint(self, endpoint):
        self._endpoint= endpoint
        
    def set_query(self, query):
        self._query= query 
    
    def set_amputationMethod(self, amputationMethod):
        self._amputationMethod=amputationMethod
    
    def set_rowNullDropPercent(self, rowNullDropPercent):
        self._rowNullDropPercent=rowNullDropPercent
    
    def set_columnNullDropPercent(self, columnNullDropPercent):
        self._columnNullDropPercent=columnNullDropPercent
    
    def set_nullReplacementMethod(self, nullReplacementMethod):
        self._nullReplacementMethod= nullReplacementMethod
        
    def set_customValueVariable(self, customValueVariable):
        self._customValueVariable= customValueVariable
        
    def set_customStringValueVariable(self, customStringValueVariable):
        self._customStringValueVariable= customStringValueVariable
        
        
    def getDataFrame(self, endpoint=None, query=None):
        """Retrieve data from a SPARQL endpoint and convert it into a Spark DataFrame.

        Args:
            endpoint (str, optional): The SPARQL endpoint URL. If not provided, the default endpoint will be used.
            query (str, optional): The SPARQL query string. If not provided, the default query will be used.

        Returns:
            pyspark.sql.DataFrame: The resulting Spark DataFrame.

        Raises:
            TypeError: If there are null values in the Pandas DataFrame and no handling method is specified.

        Notes:
            This function retrieves data from a SPARQL endpoint and converts it into a Spark DataFrame. It follows the
            following steps:

            1. If the endpoint is not provided, the default endpoint is used. If the default endpoint is not set, an error
               message is displayed and the function returns.

            2. If the query is not provided, the default query is used. If the default query is not set, an error message
               is displayed and the function returns.

            3. The data is queried from the SPARQL endpoint and converted into a Pandas DataFrame.

            4. If there are null values in the Pandas DataFrame, handling methods are applied based on the configured
               amputation method.

            5. The Pandas DataFrame is then converted into a Spark DataFrame.

            6. The resulting Spark DataFrame is returned.
        """
        
        
        #(Sparql-->PandasDF-->SparkDf)
        
        if endpoint==None:
            endpoint=self._endpoint
            if endpoint == '':
                print('Can not proceed further, please provide the enpoint')
                return
            
        if query==None:
            query=self._query
            if query == '':
                print('Can not proceed further, please provide the query')
                return
        
        #query from sparql endpoint and then create Pandas dataframe 
        pandasDataFrame=(sparql_dataframe.get(endpoint, query))


        #create Spark Dataframe from Pandas Dataframe,
        # if Null values want to be handled, enter here
        if self._handleNullValues=='True':
            if self._amputationMethod=='nullDrop':
                sparkDataFrame= DataAcquisition.sparkSession.createDataFrame(self.nullDrop(pandasDataFrame))
            
            if self._amputationMethod=='nullReplacement':
                sparkDataFrame= DataAcquisition.sparkSession.createDataFrame(self.nullReplacement(pandasDataFrame))
                
         
        #if there is Null values and raise error, force to go to handling methods
        try:
            sparkDataFrame= DataAcquisition.sparkSession.createDataFrame(pandasDataFrame)
        except TypeError as error:
            print(error)
            print('Null values exist, handling methods will be applied')
                    
            if self._amputationMethod=='nullDrop':
                sparkDataFrame= DataAcquisition.sparkSession.createDataFrame(self.nullDrop(pandasDataFrame))
            
            if self._amputationMethod=='nullReplacement':
                sparkDataFrame= DataAcquisition.sparkSession.createDataFrame(self.nullReplacement(pandasDataFrame))
                
            
        return sparkDataFrame


    def query_local_rdf(self, rdf_file_path, rdf_format, query):
        """
            Query RDF data stored locally and convert results to a Spark DataFrame.
        
            This function loads RDF data from a specified file in a given format, executes
            a SPARQL query, and converts the query results into a Spark DataFrame. The RDF
            data is first loaded into an RDFlib Graph object and then queried using the
            provided SPARQL query. The query results are processed as follows:
            
            1. The query results are converted to a list of dictionaries.
            2. A CSV representation of the results is created using the csv.DictWriter.
            3. The CSV data is read into a Pandas DataFrame.
            4. Depending on the 'amputationMethod' attribute of the 'DataAcquisition' class,
               null values in the Pandas DataFrame may be handled. If 'amputationMethod' is
               'nullDrop', null values are dropped; if 'amputationMethod' is 'nullReplacement',
               null values are replaced based on the defined strategy.
            5. Finally, a Spark DataFrame is created from the Pandas DataFrame.
        
            Args:
                self (object): The instance of the class calling this method.
                rdf_file_path (str): The path to the RDF data file.
                rdf_format (str): The format of the RDF data (e.g., "turtle", "xml", "n3").
                query (str): The SPARQL query to execute on the RDF data.
        
            Returns:
                pyspark.sql.DataFrame: A Spark DataFrame containing the query results.
        
            Raises:
                ValueError: If any of the required parameters (rdf_file_path, rdf_format, or query)
                            is missing or empty.
            
            Note:
                - This function assumes the existence of a SparkSession in the 'DataAcquisition'
                  class context.
                - Handling of null values is based on the 'amputationMethod' attribute of the
                  'DataAcquisition' class.

        """
        if not rdf_file_path or not rdf_format or not query:
            raise ValueError("All required parameters (rdf_file_path, rdf_format, and query) must be provided.")
    
        # Load RDF data from the specified file in the given format
        graph = Graph()
        graph.parse(rdf_file_path, format=rdf_format, publicID=" ")
    
    
        results = graph.query(query)
        
        # Convert query results to a list of dictionaries
        query_results = []
        fieldnames = [str(var) for var in results.vars]  # Get field names from results.vars
        for row in results:
            result_dict = dict(zip(fieldnames, row))
            query_results.append(result_dict)
    
        # Create a StringIO object to hold the CSV data
        csv_data = StringIO()
        # Create a CSV writer
        csv_writer = csv.DictWriter(csv_data, fieldnames=fieldnames)
        csv_writer.writeheader()
        csv_writer.writerows(query_results)
        csv_data.seek(0)
        
        # Read the CSV data from StringIO into a Pandas DataFrame
        pandasDataFrame = pd.read_csv(csv_data)

        #create Spark Dataframe from Pandas Dataframe,
        # if Null values wnant to be handled beforehand, enter here
        if self._handleNullValues=='True':
            if self._amputationMethod=='nullDrop':
                sparkDataFrame= DataAcquisition.sparkSession.createDataFrame(self.nullDrop(pandasDataFrame))
            
            if self._amputationMethod=='nullReplacement':
                sparkDataFrame= DataAcquisition.sparkSession.createDataFrame(self.nullReplacement(pandasDataFrame))
                
        
        #if there is Null values and rreaise error, force to go to handling methods
        try:
            sparkDataFrame= DataAcquisition.sparkSession.createDataFrame(pandasDataFrame)
        except TypeError as error:
            print(error)
            print('Null values exist, handling methods will be applied')
                    
            if self._amputationMethod=='nullDrop':
                sparkDataFrame= DataAcquisition.sparkSession.createDataFrame(self.nullDrop(pandasDataFrame))
            
            if self._amputationMethod=='nullReplacement':
                sparkDataFrame= DataAcquisition.sparkSession.createDataFrame(self.nullReplacement(pandasDataFrame))
    
        return sparkDataFrame


        
    def nullReplacement(self, df):
        """Apply null replacement methods on variables with NaN values in a DataFrame.
    
        Args:
            df (pandas.DataFrame): The input DataFrame.

        Returns:
            pandas.DataFrame: The DataFrame with null values replaced according to the specified method.

        Raises:
            ValueError: If the customStringValueVariable or customValueVariable is not defined for the 'customValue'
                        null replacement method.

        Notes:
            This function applies null replacement methods on variables with NaN values in the input DataFrame. It follows
            the following steps:

            1. If the null replacement method is set to 'median', iterate over the columns of the DataFrame that have
               null values and fill them with the column's median value. Note that this method cannot be applied to string
               columns.

            2. If the null replacement method is set to 'mean', iterate over the columns of the DataFrame that have
               null values and fill them with the column's mean value.

            3. If the null replacement method is set to 'mod' (mode/most frequent value), iterate over the columns of the
               DataFrame that have null values and fill them with the column's mode (first most frequent value). Note that
               this method cannot be applied to string columns.

            4. If the null replacement method is set to 'customValue', iterate over the columns of the DataFrame that have
               null values. If the column's data type is an object (string), fill the null values with the specified custom
               string value. If the column's data type is not an object, fill the null values with the specified custom
               numeric value.

            5. The resulting DataFrame with replaced null values is returned.
        """
        
        #fill with median
        if self._nullReplacementMethod=="median":
            for i in df.columns[df.isnull().any(axis=0)]:     
                df[i].fillna(df[i].median(),inplace=True)
        
        #fill with mean
        if self._nullReplacementMethod=="mean":
            for i in df.columns[df.isnull().any(axis=0)]:     
                df[i].fillna(df[i].mean(),inplace=True)
        
        #fill with mode(most frequent value)
        if self._nullReplacementMethod=="mod":
            for i in df.columns[df.isnull().any(axis=0)]:     
                df[i].fillna(df[i].mode().iloc[0],inplace=True)
                
        #fill with user defined value
        if self._nullReplacementMethod=="customValue":
            for i in df.columns[df.isnull().any(axis=0)]:
                if df[i].dtypes == object:
                    if self._customStringValueVariable=='':
                        print('Please define the customStringValueVariable or change the nullReplacementMethod')
                        return
                    df[i].fillna(self._customStringValueVariable,inplace=True)
                    print(f'NaN values in -{i}- column replaced by value -{self._customStringValueVariable}-')
                else:
                    if self._customValueVariable=='':
                        print('Please define the customValueVariable or change the nullReplacementMethod')
                        return
                    df[i].fillna(self._customValueVariable,inplace=True)
                    print(f'NaN values in -{i}- column replaced by value -{self._customValueVariable}-')
  
        return df
    
    
    def nullDrop(self, df):
        """Apply null dropping according to thresholds on a DataFrame.
    
        Args:
            df (pandas.DataFrame): The input DataFrame.

        Returns:
            pandas.DataFrame: The DataFrame with null values dropped according to the specified thresholds.

        Notes:
            This function applies null dropping on the input DataFrame based on the following steps:

            1. Drop columns where the percentage of missing values is greater than or equal to the specified
               `self._columnNullDropPercent` threshold.

            2. Drop rows where the percentage of missing values is greater than or equal to the specified
               `self._rowNullDropPercent` threshold.

            3. If there are still null values in the DataFrame after dropping, the nullReplacement function is called
               to apply the specified null replacement method.

            4. The resulting DataFrame with dropped null values is returned.

        Warnings:
            - The nullReplacement method will be called if there are still null values after dropping. Make sure the
              nullReplacementMethod is properly configured.

        """
        
        #first: column base, if null values are over a threshold percent
        print(f'Drop the columns where at least %{self._columnNullDropPercent} element is missing.')
        df.dropna(axis='columns',thresh=(len(df)*(self._columnNullDropPercent/100)),inplace = True)
        
        #second: row base, if null values are over a threshold percent
        print(f'Drop the rows where at least %{self._rowNullDropPercent} element is missing.')
        df.dropna(axis='index',thresh=(len(df.columns)*(self._rowNullDropPercent/100)),inplace = True)
        
        #if there are still null values, call the nullReplacement function
        if [i for i in df.columns[df.isnull().any(axis=0)]]:
            print(f'After dropping, there are still null values. {self._nullReplacementMethod} null replacement method will be applied')
            df=self.nullReplacement(df)
        
        return df  

