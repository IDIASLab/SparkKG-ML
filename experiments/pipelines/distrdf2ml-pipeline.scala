import net.sansa_stack.ml.spark.featureExtraction.{FeatureExtractingSparqlGenerator, SmartVectorAssembler, SparqlFrame}
import net.sansa_stack.query.spark.SPARQLEngine
import net.sansa_stack.rdf.common.io.riot.error.{ErrorParseMode, WarningParseMode}
import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.sys.JenaSystem
import org.apache.jena.graph.Triple
import org.apache.jena.riot.Lang
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{asc, col, count, countDistinct, desc, explode, size,concat_ws}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.regression.RandomForestRegressor
JenaSystem.init()
 
val dataset = NTripleReader.load(spark,"/FileStore/tables/linkedmdb_18_05_2009_dump.nt",stopOnBadTerm = ErrorParseMode.SKIP,stopOnWarnings = WarningParseMode.IGNORE).toDS().cache()

val sparqlString = """SELECT ?movie ?title ?date ?genre ?runtime
        WHERE {
        ?movie <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://data.linkedmdb.org/movie/film> .
        ?movie <http://purl.org/dc/terms/date> ?date .
        ?movie <http://purl.org/dc/terms/title> ?title .
        ?movie <http://data.linkedmdb.org/movie/runtime> ?runtime . 
        ?movie <http://data.linkedmdb.org/movie/genre> ?movie__down_genre . 
        ?movie__down_genre <http://data.linkedmdb.org/movie/film_genre_name> ?genre.
        }
        """

val sparqlFrame = new SparqlFrame().setSparqlQuery(sparqlString).setCollapsByKey(true).setCollapsColumnName("movie")
val collapsedExtractedFeaturesDf = sparqlFrame.transform(dataset).cache()

val filteredDf = collapsedExtractedFeaturesDf.filter(size(col("genre(ListOf_NonCategorical_String)")) <= 1)
val concatenatedDf = filteredDf.withColumn("genre", concat_ws(",", filteredDf("genre(ListOf_NonCategorical_String)")))


val smartVectorAssembler = new SmartVectorAssembler().setEntityColumn("movie").setLabelColumn("genre").setNullReplacement("string", "").setNullReplacement("digit", -1)
val assembledDf: DataFrame = smartVectorAssembler.transform(concatenatedDf).cache()

val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("IndexedLabel").fit(assembledDf)
val assembledDf2 = labelIndexer.transform(assembledDf)

val Array(trainingData, testData) = assembledDf2.randomSplit(Array(0.7, 0.3))
val rf = new RandomForestClassifier().setLabelCol("IndexedLabel").setFeaturesCol("features")
val model = rf.fit(trainingData)
val predictions = model.transform(testData)

val ml2Graph = new ML2Graph().setEntityColumn("entityID").setValueColumn("predictedLabel")
val metagraph = ml2Graph.transform(predictions)