from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.types import ArrayType, StringType, StructType, StructField
from pyspark.ml.feature import Word2Vec
from typing import List
import pyspark.sql.functions as f
from graphframes import *
from functools import reduce


class MotifWalks:
    """
    MotifWalks class generates walks on a graph for given entities, performs motif walks, and extracts embeddings 
    using Word2Vec model.

    Attributes:
        entities (List[str]): List of starting entities for Motif Walks.
        kg_instance: Instance of the knowledge graph.
        sparkSession (SparkSession): SparkSession for Spark operations.
        hashed_entities (List[str]): Hashed entities for efficient lookup.
    """

    def __init__(self, kg_instance, entities: List[str] = [], sparkSession: SparkSession = None):
        """
        Initializes the MotifWalks class.

        Parameters:
            entities (List[str]): List of starting entities for Motif Walks.
            sparkSession (SparkSession): Custom SparkSession, if provided.
            kg_instance: Instance of the knowledge graph class.
        """

        self.entities = entities
        self.kg_instance = kg_instance
        self.hashed_entities = [self.kg_instance.vertex_to_key_hashMap[e] for e in entities]

        if sparkSession is None:
            # If no custom SparkSession is provided, create a new one.
            self.sparkSession = SparkSession.builder.getOrCreate()
        else:
            # Use the provided SparkSession.
            self.sparkSession = sparkSession

    def create_motif_string(self, depth):
        """
        Generates a motif string for a given depth.

        Parameters:
            depth (int): Depth of the motif.

        Returns:
            str: Motif string for the given depth.
        """
        # Start with an empty motif
        motif = ""

        # Dynamically build the motif string based on the depth
        for i in range(depth):
            if i > 0:
                motif += "; "
            motif += f"(v{i})-[e{i}]->(v{i+1})"

        return motif

    def struct_to_list(self, df, walktype):
        """
        Transforms the struct type in a DataFrame to a list of strings based on the specified walk type.

        Parameters:
            df (DataFrame): Input DataFrame containing struct types.
            walktype (str): The type of walk to perform. Can be 'BFS', 'entity', or 'predicate'.

        Returns:
            DataFrame: Transformed DataFrame with each row represented as a list of strings.
        """

        # Broadcast the dictionary (hashMap) so every node in the Spark cluster can access it.
        broadcasted_hashMap = self.sparkSession.sparkContext.broadcast(self.kg_instance.key_to_vertex_hashMap)

        # Define a UDF that converts a struct to a list of strings.
        def to_list(row):
            # Access the broadcasted hashMap value
            hashMap = broadcasted_hashMap.value
            result = []  # Initialize an empty list to store the result
            fields = row.__fields__  # Get the fields (columns) of the struct

            # Iterate over each field in the struct
            for i in range(len(fields)):
                if i % 2 != 0:  # Check if the index is odd (which corresponds to certain fields we're interested in)
                    col = fields[i]
                    attribute = getattr(row, col)  # Get the attribute (value) of the current field
                    
                    # Handle the case where the index is 1 (typically the first relevant field)
                    if i == 1:
                        if walktype == 'BFS':
                            # For BFS: Add source, relationship, and destination to the result
                            result.append(hashMap.get(attribute.src))
                            result.append(hashMap.get(attribute.relationship))
                            result.append(hashMap.get(attribute.dst))
                        elif walktype == 'entity':
                            # For entity: Add source and destination to the result
                            result.append(hashMap.get(attribute.src))
                            result.append(hashMap.get(attribute.dst))
                        elif walktype == 'predicate':
                            # For predicate: Add source and relationship to the result
                            result.append(hashMap.get(attribute.src))
                            result.append(hashMap.get(attribute.relationship))
                    else:
                        # Handle other odd indices
                        if walktype == 'BFS':
                            # For BFS: Add relationship and destination to the result
                            result.append(hashMap.get(attribute.relationship))
                            result.append(hashMap.get(attribute.dst))
                        elif walktype == 'entity':
                            # For entity: Add destination to the result
                            result.append(hashMap.get(attribute.dst))
                        elif walktype == 'predicate':
                            # For predicate: Add relationship to the result
                            result.append(hashMap.get(attribute.relationship))
            
            return result  # Return the resulting list

        # Register the UDF with the correct return type (Array of Strings)
        to_list_udf = f.udf(to_list, ArrayType(StringType()))

        # Combine all columns into a single struct column for easier processing
        combined_df = df.select(f.struct(*[f.col(name) for name in df.columns]).alias("combined"))

        # Apply the UDF to transform the struct column into a list of strings
        paths_df = combined_df.select(to_list_udf("combined").alias("paths"))

        return paths_df  # Return the DataFrame with the transformed lists
    

    def motif_walk(self, graph, depth, walktype='BFS'):
        """
        Conducts motif walks on the given graph for the specified depth. This function 
        processes each depth level separately, allowing for more granular control over 
        path filtering, especially based on vertex properties like outgoing edges.

        Parameters:
            graph (GraphFrame): The graph on which to perform motif walks. The vertices 
                                should have a 'has_outgoing_edge' column to facilitate 
                                filtering.
            depth (int): The maximum depth (number of steps) of the motif walks.
            walktype (str): The type of walk to perform, such as 'BFS','predicate','entity'. Default is 'BFS'.

        Returns:
            DataFrame: A DataFrame containing the paths resulting from the motif walks, 
            with one row per path.

        Notes:
            - This function creates and processes motifs for each depth level from 1 to 
            the specified maximum depth, providing more refined filtering options.
            - It allows filtering of paths based on whether the last vertex in the path 
            has outgoing edges, thereby potentially terminating paths early.
        """

        # Define the schema for the resulting DataFrame, which will contain an array of strings
        schema = StructType([StructField("paths", ArrayType(StringType()))])
        
        # Create an empty DataFrame with the defined schema to store the resulting paths
        paths_df = self.sparkSession.createDataFrame([], schema)
        
        # Initialize an empty list to collect DataFrames from each depth iteration
        dataframes_list = []
        
        # Loop through each depth level from 1 to the specified maximum depth
        for i in range(1, depth + 1):
            # Create the motif string based on the current depth
            motif = self.create_motif_string(i)
            
            # Find motifs in the graph using the generated motif string
            results = graph.find(motif)
            
            # Filter the walks that start with desired entities, using the pre-hashed entities
            filtered_results = results.filter(f.col("v0.id").isin(self.hashed_entities))
            
            # If the current depth is less than the maximum depth, filter out paths 
            # where the last vertex has outgoing edges (for early termination)
            if i < depth:
                filtered_results = filtered_results.filter(~f.col(f"v{i}.has_outgoing_edge"))
            
            # Transform the filtered motifs into a DataFrame with a single column 
            # representing the path structure, formatted for Word2Vec processing
            new_df = self.struct_to_list(filtered_results, walktype)
            
            # Append the resulting DataFrame to the list of DataFrames
            dataframes_list.append(new_df)
        
        # Union all collected DataFrames from the list into a single DataFrame
        paths_df = reduce(DataFrame.union, dataframes_list)
        
        return paths_df


    def motif_walk_depth(self, graph, depth, walktype='BFS'):
        """
        Conducts motif walks on the given graph for the specified depth. This function 
        performs a motif walk across the entire specified depth in one go and returns 
        the resulting paths.
    
        Parameters:
            graph (GraphFrame): The graph on which to perform the motif walks.
            depth (int): The depth (number of steps) of the motif walks.
    
        Returns:
            DataFrame: A DataFrame containing the paths resulting from the motif walks, 
            with one row per path.
        
        Notes:
            - This function creates a single motif string for the entire depth and 
              processes the graph accordingly. It does not account for intermediate 
              filtering based on the properties of vertices encountered during the walk.
        """
        # Create the motif string based on the specified depth
        motif = self.create_motif_string(depth)
    
        # Find motifs in the graph based on the generated motif string
        results = graph.find(motif)
        
        # Filter the walks that start with desired entities, using the pre-hashed entities
        filtered_results = results.filter(f.col("v0.id").isin(self.hashed_entities))
    
        # Transform the structured motifs into a DataFrame with a single column 
        # representing the path structure, formatted for Word2Vec processing
        paths_df = self.struct_to_list(filtered_results,walktype)
    
        return paths_df

    def word2Vec_embeddings(self, df, vector_size=100, min_count=5, num_partitions=1, step_size=0.025,
                            max_iter=1, seed=None, input_col="sentences", output_col="vectors", window_size=5,
                            max_sentence_length=1000, **kwargs):
        """
        Trains a Word2Vec model on walks and returns the vectors of entities.

        Parameters:
            df (DataFrame): DataFrame containing paths for training the Word2Vec model.
            vector_size (int): Size of the word vectors.
            min_count (int): Minimum number of occurrences for a word to be included in the vocabulary.
            num_partitions (int): Number of partitions for Word2Vec estimation.
            step_size (float): Step size (learning rate) for optimization.
            max_iter (int): Maximum number of iterations for optimization.
            seed (int): Random seed for initialization.
            input_col (str): Input column name.
            output_col (str): Output column name.
            window_size (int): Size of the window for skip-gram.
            max_sentence_length (int): Maximum length of a sentence.
            **kwargs: Additional arguments for Word2Vec model.

        Returns:
            DataFrame: DataFrame with vectors of entities.
        """
        # Word2Vec model trained on sentences
        word2vec = Word2Vec(vectorSize=vector_size, minCount=min_count, numPartitions=num_partitions,
                            stepSize=step_size, maxIter=max_iter, seed=seed, inputCol=input_col,
                            outputCol=output_col, windowSize=window_size, maxSentenceLength=max_sentence_length,
                            **kwargs)
        model = word2vec.fit(df)

        # Get the vocabulary, which contains word vectors
        embeddings = model.getVectors()

        # Filter the word_vectors DataFrame to include only vectors of entities in the list
        entity_embeddings = embeddings.filter(embeddings["word"].isin(self.entities))

        return entity_embeddings