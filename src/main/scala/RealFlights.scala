import org.apache.spark.sql.{DataFrame, SparkSession}

object RealFlights {


  def main(args: Array[String]): Unit = {
    // Create sparksession and set loglevel
    val spark: SparkSession = sparkCreator()
    // Make graph
    val graph = getGraph(spark)

    graph.printSchema()
    graph.show()
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
    // Create graph
    flights
  }

}
