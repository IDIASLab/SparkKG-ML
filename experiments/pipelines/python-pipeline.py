import pandas as pd
from rdflib import Graph, URIRef
from rdflib.plugins.sparql import prepareQuery
from pyspark.ml.feature import VectorAssembler,StringIndexer,Word2Vec,Tokenizer
from pyspark.sql.functions import size,length,concat_ws,col
from pyspark.ml.classification import RandomForestClassifier

g = Graph()
g.parse("lmdb.nt", format="n3")

query_str = """
    SELECT ?movie ?title ?date ?genre ?runtime
    WHERE {
        ?movie <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://data.linkedmdb.org/movie/film> .
        ?movie <http://purl.org/dc/terms/date> ?date .
        ?movie <http://purl.org/dc/terms/title> ?title .
        ?movie <http://data.linkedmdb.org/movie/runtime> ?runtime . 
        ?movie <http://data.linkedmdb.org/movie/genre> ?movie__down_genre . 
        ?movie__down_genre <http://data.linkedmdb.org/movie/film_genre_name> ?genre.
    }
    LIMIT 100
"""

query = prepareQuery(query_str)

results = g.query(query)

data = [(str(row.movie), str(row.title), str(row.date), str(row.genre), str(row.runtime)) for row in results]
df = pd.DataFrame(data, columns=["movie", "title", "date", "genre", "runtime"])

spark_df = spark.createDataFrame(df)

spark_df = spark_df.withColumn('runtime', col('runtime').cast('double'))

indexer = StringIndexer(inputCol="genre", outputCol="genreIndex")
spark_df = indexer.fit(spark_df).transform(spark_df)

tokenizer = Tokenizer(inputCol="title", outputCol="title_words")
tokenizer_date = Tokenizer(inputCol="date", outputCol="date_words")
tokenized_df = tokenizer.transform(spark_df)
tokenized_df = tokenizer_date.transform(tokenized_df)

word2Vec = Word2Vec(vectorSize=2, minCount=1, inputCol="title_words", outputCol="title_vector")
word2Vec_date = Word2Vec(vectorSize=2, minCount=1, inputCol="date_words", outputCol="date_vector")

word2Vec_model = word2Vec.fit(tokenized_df)
word2Vec_date_model = word2Vec_date.fit(word2Vec_model.transform(tokenized_df))

transformed_df = word2Vec_date_model.transform(word2Vec_model.transform(tokenized_df))

assembled_data = VectorAssembler(inputCols=["title_vector", "date_vector", "runtime"], outputCol="features").transform(transformed_df)

train_data, test_data = assembled_data.randomSplit([0.7, 0.3])
rf_classifier = RandomForestClassifier(labelCol="genreIndex", featuresCol="features", numTrees=10)
rf_classifier = rf_classifier.fit(assembled_data)
predictions = rf_classifier.transform(test_data)

df_pandas_predictions = predictions.select("recipe","calorie", "prediction").toPandas()

uri1='recipe'
label1='calorie'
prediction1='prediction'

graph=Graph()

EX_NS = Namespace("http://example.com/")
graph.bind("my", EX_NS)
        
experimentTypePropertyURIasString = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
experimentTypeURIasString = "https://example/Vocab/experiment"
metagraphDatetime = datetime.now()
experimentHash = str(hash(str(metagraphDatetime)))
experimentNode = BNode(experimentHash)
experimentTypePropertyNode = URIRef(experimentTypePropertyURIasString)
experimentTypeNode = URIRef(experimentTypeURIasString)

graph.add((experimentNode, experimentTypePropertyNode, experimentTypeNode))
       
for index, row in df_pandas_predictions.iterrows():
    uri = row[uri1]
    label = row[label1]
    prediction = row[prediction1]

    instance_node = URIRef(uri)

    graph.add((instance_node, EX_NS.label, Literal(label)))
    graph.add((instance_node, EX_NS.prediction, Literal(prediction)))
    graph.add((experimentNode, EX_NS.hasPrediction, Literal(prediction)))
            
        
graph.serialize(destination='predictions.ttl') 