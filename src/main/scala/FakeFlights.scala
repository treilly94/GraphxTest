import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object FakeFlights {


  def main(args: Array[String]): Unit = {
    // Create sparksession and set loglevel
    val spark: SparkSession = sparkCreator()
    // Make graph
    val graph: Graph[String, Long] = getGraph(spark)


    println("All verticies")
    graph.vertices.foreach(println(_))

    println("All verticies where the second char of name is F")
    graph.vertices.filter { case (id, name) => name(1) == 'F'}.foreach(println(_))

    println("All edges where the distance is greater than 1000")
    graph.edges.filter(e => e.attr > 1000).foreach(println(_))

    println("All triplets")
    graph.triplets.foreach(println(_))

    println("All triplets in order of distance")
    graph.triplets.sortBy(t => t.attr, ascending = false).foreach(println(_))

  }


  def sparkCreator(): SparkSession = {
    val spark: SparkSession = SparkSession.builder
      .appName("FlightTest")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .getOrCreate
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }


  def getGraph(spark: SparkSession): Graph[String, Long] = {
    // Read in data
    val optionMap: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")
    val airports: DataFrame = spark.read.options(optionMap).csv("./src/main/resources/fakeFlights/airports.csv")
    val routes: DataFrame =  spark.read.options(optionMap).csv("./src/main/resources/fakeFlights/routes.csv")
    // Create graph
    val airportsVertices: RDD[(VertexId, String)] = airports.rdd.map(row => (row(0).asInstanceOf[Number].longValue,
      row(1).asInstanceOf[String]))
    val routesEdges: RDD[Edge[Long]] = routes.rdd.map(row => Edge(row(0).asInstanceOf[Number].longValue,
      row(1).asInstanceOf[Number].longValue,
      row(2).asInstanceOf[Number].longValue))

    val defaultAirport: String = "Missing Airport"

    Graph(airportsVertices, routesEdges, defaultAirport)
  }
}
