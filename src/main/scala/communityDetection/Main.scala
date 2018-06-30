package communityDetection

import org.graphframes.GraphFrame
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import wcc.GraphFrameOps._

/**
  * Created by tariq on 28/11/17.
  */

object Main {

  /* TEST:
    import org.apache.spark.graphx._
    import org.apache.spark.rdd.RDD
    val users: RDD[(VertexId, (String))] =
      sc.parallelize(Array((1L, "collab"),(2L, "collab"),
        (3L, "collab"),(4L, "collab"),(5L, "collab"),(6L, "collab")))
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(4L, 2L, "collab"),
        Edge(4L, 3L, "collab"),Edge(3L, 2L, "collab"),
        Edge(2L, 1L, "collab"),Edge(5L, 1L, "collab"),
        Edge(6L, 1L, "collab"), Edge(6L, 5L, "collab")))
    val graph = Graph(users, relationships)
   */

  val FULLRUN = 1
  val STREAM = 2

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("CommunityDetection")

    // check Spark configuration for master URL, set it to local if not configured
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("spark://claas13.local:7077")
    }

    // Set logging level if log4j not configured (override by adding log4j.properties to classpath)
//    if (!Logger.getRootLogger.getAllAppenders.hasMoreElements) {
      Logger.getRootLogger.setLevel(Level.WARN)
//    }

    Logger.getRootLogger.warn("Getting context!!")

    // Load and partition Graph
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val spark = SparkSession.builder().config(ssc.sparkContext.getConf).getOrCreate()

    Logger.getRootLogger.warn("We have context!!")

    val Array(filePath) = args.take(1)
    val operation = FULLRUN

    operation match {

      case FULLRUN =>
        loadAndRegionalizeGraph(spark, filePath)

      case STREAM =>
        CSVGraph.testStream(spark, filePath)

    }

    ssc.start()
    ssc.awaitTermination()

  }

  def loadAndRegionalizeGraph(spark: SparkSession, filePath: String, edgeCount:Double=0) = {
    val graph = if (edgeCount == 0) {
      CSVGraph.loadGraph(spark, filePath)
    } else {
      CSVGraph.loadSampleGraph(spark, filePath, edgeCount)
    }
    graph.cache()

    Logger.getRootLogger.warn("graph is loaded!!")
    Logger.getRootLogger.warn(s"vertices: ${graph.vertices.count}, edges: ${graph.edges.count}")

    // JOIN GRAPH WITH COMMUNITIES
    val communityDF = graph.scalableCommunityDetection(spark)
    val regionalizedGraph = GraphFrame(graph.vertices.join(communityDF, "id"), graph.edges).cache()
    regionalizedGraph
  }


}
