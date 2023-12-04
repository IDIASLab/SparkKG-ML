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



// Query1 (genres: Spy,Superhero)

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
        
        FILTER (?genre = 'Spy' || ?genre = 'Superhero' )
        }
        """

// run the code in a loop and append runtimes for every run 
var sparqlFrameTimes: Array[Double] = Array()
var smartVectorAssemblerTimes: Array[Double] = Array()

for (_ <- 1 to 3) {
  val sparqlFrame = new SparqlFrame()
  .setSparqlQuery(sparqlString)
  .setCollapsByKey(true)
  .setCollapsColumnName("movie")

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
  //println(s"Running time SparqlFrame: $runningTimeSparqlFrame seconds")
  //println(s"Running time SmartVectorAssembler: $runningTimeSmartVectorAssembler seconds")
}


// Print or use the results as needed
println("Sparql Frame Times: " + sparqlFrameTimes.mkString(", "))
println("Smart Vector Assembler Times: " + smartVectorAssemblerTimes.mkString(", "))


// Query2 (genres: Mystery,Romantic comedy)

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
        
        FILTER (?genre = 'Mystery' || ?genre = 'Romantic comedy' )
        }
        """


// run the code in a loop and append runtimes for every run 
var sparqlFrameTimes: Array[Double] = Array()
var smartVectorAssemblerTimes: Array[Double] = Array()

for (_ <- 1 to 3) {
  val sparqlFrame = new SparqlFrame()
  .setSparqlQuery(sparqlString)
  .setCollapsByKey(true)
  .setCollapsColumnName("movie")

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
  //println(s"Running time SparqlFrame: $runningTimeSparqlFrame seconds")
  //println(s"Running time SmartVectorAssembler: $runningTimeSmartVectorAssembler seconds")
}


// Print or use the results as needed
println("Sparql Frame Times: " + sparqlFrameTimes.mkString(", "))
println("Smart Vector Assembler Times: " + smartVectorAssemblerTimes.mkString(", "))


// Query3 (genres: Musical, Action)

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
        ((?movie__down_runtime) as ?runtime)
        WHERE { 
        ?movie <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://data.linkedmdb.org/movie/film> . 
        ?movie <http://data.linkedmdb.org/movie/genre> ?movie__down_genre . ?movie__down_genre <http://data.linkedmdb.org/movie/film_genre_name> ?genre .
       
        
        OPTIONAL { ?movie <http://purl.org/dc/terms/title> ?movie_title . } 
        OPTIONAL { ?movie <http://data.linkedmdb.org/movie/runtime> ?movie__down_runtime . } 
        OPTIONAL { ?movie <http://data.linkedmdb.org/movie/actor> ?movie__down_actor . ?movie__down_actor <http://data.linkedmdb.org/movie/actor_name> ?actor . } 
        OPTIONAL { ?movie <http://data.linkedmdb.org/movie/director> ?movie__down_director .  ?movie__down_director <http://data.linkedmdb.org/movie/director_name> ?director . } 
        
        FILTER (?genre = 'Musical' || ?genre = 'Action' )
        }
        """


// run the code in a loop and append runtimes for every run 
var sparqlFrameTimes: Array[Double] = Array()
var smartVectorAssemblerTimes: Array[Double] = Array()

for (_ <- 1 to 3) {
  val sparqlFrame = new SparqlFrame()
  .setSparqlQuery(sparqlString)
  .setCollapsByKey(true)
  .setCollapsColumnName("movie")

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
  //println(s"Running time SparqlFrame: $runningTimeSparqlFrame seconds")
  //println(s"Running time SmartVectorAssembler: $runningTimeSmartVectorAssembler seconds")
}


// Print or use the results as needed
println("Sparql Frame Times: " + sparqlFrameTimes.mkString(", "))
println("Smart Vector Assembler Times: " + smartVectorAssemblerTimes.mkString(", "))


// Query4 (genres: Horror Film, Silent film)

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
        ((?movie__down_runtime) as ?runtime)
        WHERE { 
        ?movie <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://data.linkedmdb.org/movie/film> . 
        ?movie <http://data.linkedmdb.org/movie/genre> ?movie__down_genre . ?movie__down_genre <http://data.linkedmdb.org/movie/film_genre_name> ?genre .
       
        
        OPTIONAL { ?movie <http://purl.org/dc/terms/title> ?movie_title . } 
        OPTIONAL { ?movie <http://data.linkedmdb.org/movie/runtime> ?movie__down_runtime . } 
        OPTIONAL { ?movie <http://data.linkedmdb.org/movie/actor> ?movie__down_actor . ?movie__down_actor <http://data.linkedmdb.org/movie/actor_name> ?actor . } 
        OPTIONAL { ?movie <http://data.linkedmdb.org/movie/director> ?movie__down_director .  ?movie__down_director <http://data.linkedmdb.org/movie/director_name> ?director . } 
        
        FILTER (?genre = 'Horror Film' || ?genre = 'Silent film' )
        }
        """


// run the code in a loop and append runtimes for every run 
var sparqlFrameTimes: Array[Double] = Array()
var smartVectorAssemblerTimes: Array[Double] = Array()

for (_ <- 1 to 3) {
  val sparqlFrame = new SparqlFrame()
  .setSparqlQuery(sparqlString)
  .setCollapsByKey(true)
  .setCollapsColumnName("movie")

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
  //println(s"Running time SparqlFrame: $runningTimeSparqlFrame seconds")
  //println(s"Running time SmartVectorAssembler: $runningTimeSmartVectorAssembler seconds")
}


// Print or use the results as needed
println("Sparql Frame Times: " + sparqlFrameTimes.mkString(", "))
println("Smart Vector Assembler Times: " + smartVectorAssemblerTimes.mkString(", "))


// Query5 (genres: Drama, Black-and-white)

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
        ((?movie__down_runtime) as ?runtime)
        WHERE { 
        ?movie <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://data.linkedmdb.org/movie/film> . 
        ?movie <http://data.linkedmdb.org/movie/genre> ?movie__down_genre . ?movie__down_genre <http://data.linkedmdb.org/movie/film_genre_name> ?genre .
       
        
        OPTIONAL { ?movie <http://purl.org/dc/terms/title> ?movie_title . } 
        OPTIONAL { ?movie <http://data.linkedmdb.org/movie/runtime> ?movie__down_runtime . } 
        OPTIONAL { ?movie <http://data.linkedmdb.org/movie/actor> ?movie__down_actor . ?movie__down_actor <http://data.linkedmdb.org/movie/actor_name> ?actor . } 
        OPTIONAL { ?movie <http://data.linkedmdb.org/movie/director> ?movie__down_director .  ?movie__down_director <http://data.linkedmdb.org/movie/director_name> ?director . } 
        
        FILTER (?genre = 'Drama' || ?genre = 'Black-and-white' )
        }
        """


// run the code in a loop and append runtimes for every run 
var sparqlFrameTimes: Array[Double] = Array()
var smartVectorAssemblerTimes: Array[Double] = Array()

for (_ <- 1 to 3) {
  val sparqlFrame = new SparqlFrame()
  .setSparqlQuery(sparqlString)
  .setCollapsByKey(true)
  .setCollapsColumnName("movie")

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
  //println(s"Running time SparqlFrame: $runningTimeSparqlFrame seconds")
  //println(s"Running time SmartVectorAssembler: $runningTimeSmartVectorAssembler seconds")
}


// Print or use the results as needed
println("Sparql Frame Times: " + sparqlFrameTimes.mkString(", "))
println("Smart Vector Assembler Times: " + smartVectorAssemblerTimes.mkString(", "))
