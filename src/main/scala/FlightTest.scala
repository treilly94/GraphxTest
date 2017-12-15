import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object FlightTest {


  def main(args: Array[String]): Unit = {
    // Create sparksession and set loglevel
    val spark : SparkSession = sparkCreator()
    // Read in data
    val optionMap: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")
    val airports: DataFrame = spark.read.options(optionMap).csv("./src/main/resources/flights/airports.csv")
    val routes: DataFrame =  spark.read.options(optionMap).csv("./src/main/resources/flights/routes.csv")
    // Create graph
    val airportsVertices: RDD[(VertexId, String)] = airports.rdd.map(row => (row(0).asInstanceOf[Number].longValue,
                                                                             row(1).asInstanceOf[String]))
    val routesEdges: RDD[Edge[Long]] = routes.rdd.map(row => Edge(row(0).asInstanceOf[Number].longValue,
                                                                  row(1).asInstanceOf[Number].longValue,
                                                                  row(2).asInstanceOf[Number].longValue))

    val defaultAirport: String = "Missing Airport"

    val graph: Graph[String, Long] = Graph(airportsVertices, routesEdges, defaultAirport)
    // Print all verticies
    graph.vertices.foreach(println(_))
    // Print all verticies where name = SFO
    graph.vertices.filter { case (id, name) => name(1) == 'F'}.foreach(println(_))
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
}
