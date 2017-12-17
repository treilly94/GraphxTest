import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object RealFlights {


  def main(args: Array[String]): Unit = {
    // Create sparksession and set loglevel
    val spark: SparkSession = sparkCreator()
    // Make graph
    val graph = getGraph(spark)

    graph.triplets.foreach(println(_))
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


  def getGraph(spark: SparkSession) = {
    // Read in data
    val optionMap: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")
    val flights: DataFrame = spark.read.options(optionMap)
      .csv("./src/main/resources/realFlights/394552408_T_ONTIME.csv")
      .drop("_c17")
    // Get unique airports
    val airportsRdd: RDD[(VertexId, String)] =
      flights
        .rdd
        .map(row => (row(5).asInstanceOf[Number].longValue, row(6).asInstanceOf[String]))
          .union(flights
            .rdd
            .map(row => (row(7).asInstanceOf[Number].longValue, row(8).asInstanceOf[String]))
          ).distinct()
    // Get routes
    val routesRdd: RDD[Edge[Long]] =
      flights
        .rdd
        .map(row => Edge(row(5).asInstanceOf[Number].longValue,
          row(7).asInstanceOf[Number].longValue,
          row(16).asInstanceOf[Number].longValue))
    // Create graph
    val default: String = "Missing Airport"
    Graph(airportsRdd, routesRdd, default)
  }

}
