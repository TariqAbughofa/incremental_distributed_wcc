package communityDetection

import org.apache.log4j.Logger
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.graphframes.GraphFrame
import org.apache.spark.sql.functions._
import wcc.IncrementalDW
import java.io._

/**
  * Created by tariq on 02/05/18.
  */
object CSVGraph {

  def loadGraph(spark: SparkSession, graphFileHadoopPath: String) = {
    val edges = spark.read.option("delimiter", "\t").schema(StructType(Seq(
      StructField(name = "src", dataType = LongType, nullable = false),
      StructField(name = "dst", dataType = LongType, nullable = false)
    ))).csv(s"hdfs://claas11.local$graphFileHadoopPath")
      .filter(x => x.get(0) != null && x.getLong(0) != x.getLong(1))
    Logger.getRootLogger.warn(s"raw edges: ${edges.count}")
    GraphFrame.fromEdges(edges)
  }

  def loadSampleGraph(spark: SparkSession, graphFileHadoopPath: String, edgeCount: Double) = {
    val edges = spark.read.option("delimiter", "\t").schema(StructType(Seq(
      StructField(name = "src", dataType = LongType, nullable = false),
      StructField(name = "dst", dataType = LongType, nullable = false)
    ))).csv(s"hdfs://claas11.local$graphFileHadoopPath")
      .filter(x => x.get(0) != null && x.getLong(0) != x.getLong(1))
      .limit((edgeCount * 1000000).toInt)
    edges.show()
    Logger.getRootLogger.warn(s"raw edges: ${edges.count}")
    GraphFrame.fromEdges(edges)
  }

  def testStream(spark: SparkSession, graphFileHadoopPath: String,
                 bulkToStreamRatio: Float = .80f,
                 microBatchCount: Int = 10
                ) = {
    val edges = spark.read.option("delimiter", "\t").schema(StructType(Seq(
      StructField(name = "src", dataType = LongType, nullable = false),
      StructField(name = "dst", dataType = LongType, nullable = false)
    ))).csv(s"hdfs://claas11.local$graphFileHadoopPath")
      .filter(x => x.get(0) != null && x.getLong(0) != x.getLong(1)).cache()
    val maxVertex = edges.agg(max("dst")).head.getLong(0)
    val splitVertex = Math.floor(maxVertex * bulkToStreamRatio)
    val bulkEdges = edges.filter(x => x.getLong(0) < splitVertex && x.getLong(1) < splitVertex).cache()
    val streamEdges = edges.filter(x => x.getLong(0) >= splitVertex || x.getLong(1) >= splitVertex).cache()
    Logger.getRootLogger.warn(s"raw bulk edges: ${bulkEdges.count}")
    Logger.getRootLogger.warn(s"raw stream edges: ${streamEdges.count}")
    val graph = GraphFrame.fromEdges(bulkEdges).toGraphX

    var (itGraph, cStats) = IncrementalDW.prepare(graph, spark.sparkContext)
    itGraph.cache()
    itGraph.vertices.count()

    val microBatchSize = Math.floor((maxVertex - splitVertex) / microBatchCount)
    (1 to microBatchCount).foreach(microBatchNum => {
      val lowerLimit = splitVertex + (microBatchNum - 1) * microBatchSize
      val higherLimit = if (microBatchNum == microBatchCount)
        maxVertex + 1
      else
        splitVertex + microBatchNum * microBatchSize
      val microBatch = streamEdges.filter(e => {
        (e.getLong(0) >= lowerLimit || e.getLong(1) >= lowerLimit) &&
        e.getLong(0) < higherLimit && e.getLong(1) < higherLimit
      }).rdd.map(row => Edge(row.getAs[VertexId](0), row.getAs[VertexId](1), row))
      val microBatchEdges = EdgeRDD.fromEdges(microBatch).cache()
      microBatchEdges.count
      IncrementalDW.run(itGraph, cStats, microBatchEdges, spark.sparkContext) match {
        case (g, s) => itGraph = g; cStats = s
      }
    })

    val biggestCommunities = cStats.toList.sortWith(_._2.r > _._2.r).take(3).map(_._1)
    val list = itGraph.collectNeighborIds(EdgeDirection.Either).filter(x => biggestCommunities.contains(x._1)).collectAsMap()

    val vs = spark.read.option("delimiter", ";").schema(StructType(Seq(
      StructField(name = "id", dataType = LongType, nullable = false),
      StructField(name = "title", dataType = StringType, nullable = false)
    ))).csv(s"hdfs://claas11.local/meta.csv")
    val vss = GraphFrame(vs, edges).toGraphX.vertices

    list.foreach { case (vId, neighborIds) =>
      val subGraph = itGraph.subgraph(edge => {
        neighborIds.contains(edge.srcId) || neighborIds.contains(edge.dstId)
      }, (id, vData) => {
        vData.cId == vId
      }).outerJoinVertices(vss)((vId, _, label) => {
        label.get.getString(1)
      })

      var pw = new PrintWriter(new File(s"cm-$vId.vertices.csv"))
      pw.write("Id;Label\n")
      subGraph.vertices.collect().foreach(vertex => pw.write(s"${vertex._1};${vertex._2}\n"))
      pw.close()

      pw = new PrintWriter(new File(s"cm-$vId.edges.csv"))
      pw.write("Source;Target\n")
      subGraph.edges.collect().foreach(edge => pw.write(s"${edge.srcId};${edge.dstId}\n"))
      pw.close()
    }

  }

}
