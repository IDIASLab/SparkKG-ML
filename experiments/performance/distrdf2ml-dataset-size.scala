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
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
import org.apache.spark.sql.functions.{asc, col, count, countDistinct, desc, explode, size,concat_ws}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
JenaSystem.init()


// Dataset Size 1 

// read the data
val inputpath = "/FileStore/tables/sampleMovieRDF10.ttl"
val dataset = NTripleReader.load(
      spark,
      inputpath,
      stopOnBadTerm = ErrorParseMode.SKIP,
      stopOnWarnings = WarningParseMode.IGNORE
    ).toDS().cache()

// preapare the sparql query
val sparqlString = """SELECT  ?movie ?producer ?date ?actor
        WHERE {
        ?actor  <file:///databricks/driver/movie_information.org/actedIn> ?movie.
        ?movie <file:///databricks/driver/movie_information.org/publishedDate> ?date.
        ?movie <file:///databricks/driver/movie_information.org/producedBy> ?producer.
        } 
        """

// run the code in a loop and append runtimes for every run 
var sparqlFrameTimes: Array[Double] = Array()
var smartVectorAssemblerTimes: Array[Double] = Array()

for (_ <- 1 to 3) {
  val sparqlFrame = new SparqlFrame()
  .setSparqlQuery(sparqlString)
  .setCollapsByKey(true)
  //.setCollapsColumnName("movie")

  val startTimeSparqlFrame = System.nanoTime() // Record the start time
  val collapsedExtractedFeaturesDf = sparqlFrame
    .transform(dataset)
    .cache()
  val endTimeSparqlFrame = System.nanoTime() // Record the end time
  // Calculate and print the running time in seconds
  val runningTimeSparqlFrame = (endTimeSparqlFrame - startTimeSparqlFrame) / 1.0e9


  val smartVectorAssembler = new SmartVectorAssembler()
    .setNullReplacement("string", "")
    .setNullReplacement("digit", -1)
    //.setEntityColumn("movie")
    //.setLabelColumn("genre_name")
    

  val startTimeSmartVectorAssembler = System.nanoTime() // Record the start time
  val assembledDf: DataFrame = smartVectorAssembler
  .transform(collapsedExtractedFeaturesDf)
  .cache()
  val endTimeSmartVectorAssembler = System.nanoTime() // Record the end time
  val runningTimeSmartVectorAssembler = (endTimeSmartVectorAssembler - startTimeSmartVectorAssembler) / 1.0e9

  sparqlFrameTimes = sparqlFrameTimes :+ runningTimeSparqlFrame
  smartVectorAssemblerTimes = smartVectorAssemblerTimes :+ runningTimeSmartVectorAssembler
  println("runningTimeSparqlFrame: " + runningTimeSparqlFrame)
  println("runningTimeSmartVectorAssembler: " + runningTimeSmartVectorAssembler)

}
println(sparqlFrameTimes.mkString(", "))
println(smartVectorAssemblerTimes.mkString(", "))
//println("sparqlFrameTimes: " + sparqlFrameTimes)
//println("smartVectorAssemblerTimes: " + smartVectorAssemblerTimes)


// Dataset Size 2

// read the data
val inputpath = "/FileStore/tables/sampleMovieRDF100.ttl"
val dataset = NTripleReader.load(
      spark,
      inputpath,
      stopOnBadTerm = ErrorParseMode.SKIP,
      stopOnWarnings = WarningParseMode.IGNORE
    ).toDS().cache()

// preapare the sparql query
val sparqlString = """SELECT  ?movie ?producer ?date ?actor
        WHERE {
        ?actor  <file:///databricks/driver/movie_information.org/actedIn> ?movie.
        ?movie <file:///databricks/driver/movie_information.org/publishedDate> ?date.
        ?movie <file:///databricks/driver/movie_information.org/producedBy> ?producer.
        } 
        """

// run the code in a loop and append runtimes for every run 
var sparqlFrameTimes: Array[Double] = Array()
var smartVectorAssemblerTimes: Array[Double] = Array()

for (_ <- 1 to 3) {
  val sparqlFrame = new SparqlFrame()
  .setSparqlQuery(sparqlString)
  .setCollapsByKey(true)
  //.setCollapsColumnName("movie")

  val startTimeSparqlFrame = System.nanoTime() // Record the start time
  val collapsedExtractedFeaturesDf = sparqlFrame
    .transform(dataset)
    .cache()
  val endTimeSparqlFrame = System.nanoTime() // Record the end time
  // Calculate and print the running time in seconds
  val runningTimeSparqlFrame = (endTimeSparqlFrame - startTimeSparqlFrame) / 1.0e9


  val smartVectorAssembler = new SmartVectorAssembler()
    .setNullReplacement("string", "")
    .setNullReplacement("digit", -1)
    //.setEntityColumn("movie")
    //.setLabelColumn("genre_name")
    

  val startTimeSmartVectorAssembler = System.nanoTime() // Record the start time
  val assembledDf: DataFrame = smartVectorAssembler
  .transform(collapsedExtractedFeaturesDf)
  .cache()
  val endTimeSmartVectorAssembler = System.nanoTime() // Record the end time
  val runningTimeSmartVectorAssembler = (endTimeSmartVectorAssembler - startTimeSmartVectorAssembler) / 1.0e9

  sparqlFrameTimes = sparqlFrameTimes :+ runningTimeSparqlFrame
  smartVectorAssemblerTimes = smartVectorAssemblerTimes :+ runningTimeSmartVectorAssembler
  println("runningTimeSparqlFrame: " + runningTimeSparqlFrame)
  println("runningTimeSmartVectorAssembler: " + runningTimeSmartVectorAssembler)

}
println(sparqlFrameTimes.mkString(", "))
println(smartVectorAssemblerTimes.mkString(", "))


// Dataset Size 3

// read the data
val inputpath = "/FileStore/tables/sampleMovieRDF1000.ttl"
val dataset = NTripleReader.load(
      spark,
      inputpath,
      stopOnBadTerm = ErrorParseMode.SKIP,
      stopOnWarnings = WarningParseMode.IGNORE
    ).toDS().cache()

// preapare the sparql query
val sparqlString = """SELECT  ?movie ?producer ?date ?actor
        WHERE {
        ?actor  <file:///databricks/driver/movie_information.org/actedIn> ?movie.
        ?movie <file:///databricks/driver/movie_information.org/publishedDate> ?date.
        ?movie <file:///databricks/driver/movie_information.org/producedBy> ?producer.
        } 
        """

// run the code in a loop and append runtimes for every run 
var sparqlFrameTimes: Array[Double] = Array()
var smartVectorAssemblerTimes: Array[Double] = Array()

for (_ <- 1 to 3) {
  val sparqlFrame = new SparqlFrame()
  .setSparqlQuery(sparqlString)
  .setCollapsByKey(true)
  //.setCollapsColumnName("movie")

  val startTimeSparqlFrame = System.nanoTime() // Record the start time
  val collapsedExtractedFeaturesDf = sparqlFrame
    .transform(dataset)
    .cache()
  val endTimeSparqlFrame = System.nanoTime() // Record the end time
  // Calculate and print the running time in seconds
  val runningTimeSparqlFrame = (endTimeSparqlFrame - startTimeSparqlFrame) / 1.0e9


  val smartVectorAssembler = new SmartVectorAssembler()
    .setNullReplacement("string", "")
    .setNullReplacement("digit", -1)
    //.setEntityColumn("movie")
    //.setLabelColumn("genre_name")
    

  val startTimeSmartVectorAssembler = System.nanoTime() // Record the start time
  val assembledDf: DataFrame = smartVectorAssembler
  .transform(collapsedExtractedFeaturesDf)
  .cache()
  val endTimeSmartVectorAssembler = System.nanoTime() // Record the end time
  val runningTimeSmartVectorAssembler = (endTimeSmartVectorAssembler - startTimeSmartVectorAssembler) / 1.0e9

  sparqlFrameTimes = sparqlFrameTimes :+ runningTimeSparqlFrame
  smartVectorAssemblerTimes = smartVectorAssemblerTimes :+ runningTimeSmartVectorAssembler
  println("runningTimeSparqlFrame: " + runningTimeSparqlFrame)
  println("runningTimeSmartVectorAssembler: " + runningTimeSmartVectorAssembler)

}
println(sparqlFrameTimes.mkString(", "))
println(smartVectorAssemblerTimes.mkString(", "))


// Dataset Size 4

// read the data
val inputpath = "/FileStore/tables/sampleMovieRDF10000.ttl"
val dataset = NTripleReader.load(
      spark,
      inputpath,
      stopOnBadTerm = ErrorParseMode.SKIP,
      stopOnWarnings = WarningParseMode.IGNORE
    ).toDS().cache()

// preapare the sparql query
val sparqlString = """SELECT  ?movie ?producer ?date ?actor
        WHERE {
        ?actor  <file:///databricks/driver/movie_information.org/actedIn> ?movie.
        ?movie <file:///databricks/driver/movie_information.org/publishedDate> ?date.
        ?movie <file:///databricks/driver/movie_information.org/producedBy> ?producer.
        } 
        """

// run the code in a loop and append runtimes for every run 
var sparqlFrameTimes: Array[Double] = Array()
var smartVectorAssemblerTimes: Array[Double] = Array()

for (_ <- 1 to 3) {
  val sparqlFrame = new SparqlFrame()
  .setSparqlQuery(sparqlString)
  .setCollapsByKey(true)
  //.setCollapsColumnName("movie")

  val startTimeSparqlFrame = System.nanoTime() // Record the start time
  val collapsedExtractedFeaturesDf = sparqlFrame
    .transform(dataset)
    .cache()
  val endTimeSparqlFrame = System.nanoTime() // Record the end time
  // Calculate and print the running time in seconds
  val runningTimeSparqlFrame = (endTimeSparqlFrame - startTimeSparqlFrame) / 1.0e9


  val smartVectorAssembler = new SmartVectorAssembler()
    .setNullReplacement("string", "")
    .setNullReplacement("digit", -1)
    //.setEntityColumn("movie")
    //.setLabelColumn("genre_name")
    

  val startTimeSmartVectorAssembler = System.nanoTime() // Record the start time
  val assembledDf: DataFrame = smartVectorAssembler
  .transform(collapsedExtractedFeaturesDf)
  .cache()
  val endTimeSmartVectorAssembler = System.nanoTime() // Record the end time
  val runningTimeSmartVectorAssembler = (endTimeSmartVectorAssembler - startTimeSmartVectorAssembler) / 1.0e9

  sparqlFrameTimes = sparqlFrameTimes :+ runningTimeSparqlFrame
  smartVectorAssemblerTimes = smartVectorAssemblerTimes :+ runningTimeSmartVectorAssembler
  println("runningTimeSparqlFrame: " + runningTimeSparqlFrame)
  println("runningTimeSmartVectorAssembler: " + runningTimeSmartVectorAssembler)

}
println(sparqlFrameTimes.mkString(", "))
println(smartVectorAssemblerTimes.mkString(", "))

// Dataset Size 5

// read the data
val inputpath = "/FileStore/tables/sampleMovieRDF100000.ttl"
val dataset = NTripleReader.load(
      spark,
      inputpath,
      stopOnBadTerm = ErrorParseMode.SKIP,
      stopOnWarnings = WarningParseMode.IGNORE
    ).toDS().cache()

// preapare the sparql query
val sparqlString = """SELECT  ?movie ?producer ?date ?actor
        WHERE {
        ?actor  <file:///databricks/driver/movie_information.org/actedIn> ?movie.
        ?movie <file:///databricks/driver/movie_information.org/publishedDate> ?date.
        ?movie <file:///databricks/driver/movie_information.org/producedBy> ?producer.
        } 
        """

// run the code in a loop and append runtimes for every run 
var sparqlFrameTimes: Array[Double] = Array()
var smartVectorAssemblerTimes: Array[Double] = Array()

for (_ <- 1 to 3) {
  val sparqlFrame = new SparqlFrame()
  .setSparqlQuery(sparqlString)
  .setCollapsByKey(true)
  //.setCollapsColumnName("movie")

  val startTimeSparqlFrame = System.nanoTime() // Record the start time
  val collapsedExtractedFeaturesDf = sparqlFrame
    .transform(dataset)
    .cache()
  val endTimeSparqlFrame = System.nanoTime() // Record the end time
  // Calculate and print the running time in seconds
  val runningTimeSparqlFrame = (endTimeSparqlFrame - startTimeSparqlFrame) / 1.0e9


  val smartVectorAssembler = new SmartVectorAssembler()
    .setNullReplacement("string", "")
    .setNullReplacement("digit", -1)
    //.setEntityColumn("movie")
    //.setLabelColumn("genre_name")
    

  val startTimeSmartVectorAssembler = System.nanoTime() // Record the start time
  val assembledDf: DataFrame = smartVectorAssembler
  .transform(collapsedExtractedFeaturesDf)
  .cache()
  val endTimeSmartVectorAssembler = System.nanoTime() // Record the end time
  val runningTimeSmartVectorAssembler = (endTimeSmartVectorAssembler - startTimeSmartVectorAssembler) / 1.0e9

  sparqlFrameTimes = sparqlFrameTimes :+ runningTimeSparqlFrame
  smartVectorAssemblerTimes = smartVectorAssemblerTimes :+ runningTimeSmartVectorAssembler
  println("runningTimeSparqlFrame: " + runningTimeSparqlFrame)
  println("runningTimeSmartVectorAssembler: " + runningTimeSmartVectorAssembler)

}
println(sparqlFrameTimes.mkString(", "))
println(smartVectorAssemblerTimes.mkString(", "))
