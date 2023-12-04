from sparkkgml.data_acquisition import DataAcquisition
from sparkkgml.feature_engineering import FeatureEngineering
from sparkkgml.vectorization import Vectorization
from sparkkgml.semantification import Semantification
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import size,length,concat_ws,col
from pyspark.ml.regression import RandomForestClassifier

query ="""
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

dataAcquisitionObject=DataAcquisition(spark)
df = dataAcquisitionObject.query_local_rdf("lmdb.nt", rdf_format='n3', query=query)

featureEngineeringObject=FeatureEngineering()
df2,features=featureEngineeringObject.getFeatures(df)

postprocessedDf = df2.filter(size(df2.genre) <= 1).withColumn('genre', concat_ws(',', df2['genre']))
features['genre']['isListOfEntries']=False

vectorizationObject=Vectorization()
digitized_df=vectorizationObject.vectorize(postprocessedDf,features)

assembled_data = VectorAssembler(inputCols=["title", "date", "runtime"], outputCol="features").transform(digitized_df)

train_data, test_data = assembled_data.randomSplit([0.7, 0.3])
rf_classifier = RandomForestClassifier(labelCol="genre", featuresCol="features", numTrees=10)
rf_classifier = rf.fit(assembled_data)
predictions = rf_classifier.transform(test_data)

semantificationObject=Semantification()
semantificationObject.semantify(predictions, uri1='movie', label1='r', prediction1='prediction', dest='experiment.ttl')