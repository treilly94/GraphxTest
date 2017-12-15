import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Graphx {

  def main(args: Array[String]): Unit = {
    // Create spark context
    val conf = new SparkConf().setAppName("SparkTests").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    // Create verticies RDD
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((1L, ("keith", "student")), (2L, ("bob", "postdoc")),
                           (3L, ("geoff", "prof")), (4L, ("dave", "prof"))))
    // Create edges RDD
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(1L, 2L, "collab"),    Edge(2L, 4L, "advisor"),
                           Edge(3L, 4L, "colleague"), Edge(1L, 3L, "nemesis")))
    // Create default user
    val defaultUser: (String, String) = ("John Doe", "Missing")
    // build the graph
    val graph: Graph[(String, String), String] = Graph(users, relationships, defaultUser)

    // Count all postdocs
    println("The number of postdocs is:")
    println(graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc"}.count())
    // Count all edges where src > dst
    println("The number of edges where the srd > dst")
    println(graph.edges.filter(e => e.srcId > e.dstId).count())

    // Create triplets
    val facts: RDD[String] =
      graph.triplets.map(triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
    facts.collect().foreach(println(_))
  }
}
