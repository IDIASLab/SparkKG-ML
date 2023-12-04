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
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
import org.apache.spark.sql.functions.{asc, col, count, countDistinct, desc, explode, size,concat_ws}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.{RegressionEvaluator}
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
JenaSystem.init()

// read the data
val inputpath = "/FileStore/tables/linkedmdb_18_05_2009_dump.nt"
val dataset = NTripleReader.load(
      spark,
      inputpath,
      stopOnBadTerm = ErrorParseMode.SKIP,
      stopOnWarnings = WarningParseMode.IGNORE
    ).toDS().cache()

// preapare the sparql query
val sparqlString = """SELECT
        ?movie
        ?movie_title
        ?genre
        ?director
        ?actor
        (<http://www.w3.org/2001/XMLSchema#int>(?movie__down_runtime) as ?runtime)
        WHERE { 
        ?movie <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://data.linkedmdb.org/movie/film> . 
        ?movie <http://data.linkedmdb.org/movie/genre> ?movie__down_genre . ?movie__down_genre <http://data.linkedmdb.org/movie/film_genre_name> ?genre .
       
        
        OPTIONAL { ?movie <http://purl.org/dc/terms/title> ?movie_title . } 
        OPTIONAL { ?movie <http://data.linkedmdb.org/movie/runtime> ?movie__down_runtime . } 
        OPTIONAL { ?movie <http://data.linkedmdb.org/movie/actor> ?movie__down_actor . ?movie__down_actor <http://data.linkedmdb.org/movie/actor_name> ?actor . } 
        OPTIONAL { ?movie <http://data.linkedmdb.org/movie/director> ?movie__down_director .  ?movie__down_director <http://data.linkedmdb.org/movie/director_name> ?director . } 
        
        FILTER (?genre = 'Spy' || ?genre = 'Superhero' || ?genre = 'Parody' || ?genre = 'Zombie' )
        }
        """

// run the code in a loop and append runtimes and accuracies for every run  

// arrays to store the results
var sparqlFrameTimes: Array[Double] = Array()
var smartVectorAssemblerTimes: Array[Double] = Array()
var accuracies: Array[Double] = Array()


for (_ <- 1 to 5) {

  val sparqlFrame = new SparqlFrame()
    .setSparqlQuery(sparqlString)
    .setCollapsByKey(true)
    .setCollapsColumnName("movie")
  
  val startTimeSparqlFrame = System.nanoTime() // Record the start time
  val collapsedExtractedFeaturesDf = sparqlFrame
    .transform(dataset)
    .cache()
  val endTimeSparqlFrame = System.nanoTime() // Record the end time
  val runningTimeSparqlFrame = (endTimeSparqlFrame - startTimeSparqlFrame) / 1.0e9

  // apply preprocess to transform genres to have only 1 genre per movie
  val filteredDf = collapsedExtractedFeaturesDf.filter(size(col("genre(ListOf_Categorical_String)")) <= 1)
  // concatenate the list elements into a comma-separated string and create a new column "genre_name"
  val concatenatedDf = filteredDf.withColumn("genre(Single_Categorical_String)", concat_ws(",", filteredDf("genre(ListOf_Categorical_String)")))
  // drop the original column "movie__down_genre__down_film_genre_name(ListOf_Categorical_String)"
  val collapsedExtractedFeaturesDf2 = concatenatedDf.drop("genre(ListOf_Categorical_String)")

  val smartVectorAssembler = new SmartVectorAssembler()
    .setEntityColumn("movie")
    .setLabelColumn("genre(Single_Categorical_String)")
    .setNullReplacement("string", "")
    .setNullReplacement("digit", -1)
    .setWord2VecSize(2)
    .setWord2VecMinCount(1)
  
  val startTimeSmartVectorAssembler = System.nanoTime() // Record the start time
  val assembledDf: DataFrame = smartVectorAssembler
  .transform(collapsedExtractedFeaturesDf2)
  .cache()
  val endTimeSmartVectorAssembler = System.nanoTime() // Record the end time
  val runningTimeSmartVectorAssembler = (endTimeSmartVectorAssembler - startTimeSmartVectorAssembler) / 1.0e9


  val labelIndexer = new StringIndexer()
    .setInputCol("label")
    .setOutputCol("indexedLabel")
    .fit(assembledDf)

  // split the data into training and test sets (30% held out for testing).
  val Array(trainingData, testData) = assembledDf.randomSplit(Array(0.7, 0.3))
  
  // train a RandomForest model.
  val rf = new RandomForestClassifier()
    .setLabelCol("indexedLabel")
    .setFeaturesCol("features")
    .setNumTrees(10)

  // chain indexers and forest in a Pipeline.
  val pipeline = new Pipeline()
    .setStages(Array(labelIndexer, rf))
  
  // train model. This also runs the indexers.
  val model = pipeline.fit(trainingData)
  
  val predictions = model.transform(trainingData)
  val evaluator = new MulticlassClassificationEvaluator()
        .setLabelCol("indexedLabel")
        .setPredictionCol("prediction")
        .setMetricName("accuracy")
  val accuracy = evaluator.evaluate(predictions)

  
  val predictions2 = model.transform(testData)
  val evaluator2 = new MulticlassClassificationEvaluator()
        .setLabelCol("indexedLabel")
        .setPredictionCol("prediction")
        .setMetricName("accuracy")
  val accuracy2 = evaluator2.evaluate(predictions2)

  // add the times and accuracy to the arrays
  sparqlFrameTimes = sparqlFrameTimes :+ runningTimeSparqlFrame
  smartVectorAssemblerTimes = smartVectorAssemblerTimes :+ runningTimeSmartVectorAssembler
  accuracies = accuracies :+ accuracy2

}

// print or use the results as needed
println("Sparql Frame Times: " + sparqlFrameTimes.mkString(", "))
println("Smart Vector Assembler Times: " + smartVectorAssemblerTimes.mkString(", "))
println("Accuracies: " + accuracies.mkString(", "))
