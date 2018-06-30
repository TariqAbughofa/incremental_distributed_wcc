package wcc

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.reflect.ClassTag

/**
  * Created by tariq on 06/01/18.
  */
object GraphXOps {
  implicit class GXOperations[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]) {

    def scalableCommunityDetection(sc: SparkContext): RDD[Row] = {
      DistributedWCC.run(graph, sc, isCanonical = true)
        .map{ case(id, cId) => Row.fromSeq(Array(id, cId)) }
    }

    /**
      * Partition the graph in a way that guarantees all vertices connected from
      * a community C1 to a community C2 will all belong to the same partition.
      * @param numCommunities
      * @param cId
      * @return
      */
    def partitionByCommunity(numCommunities: Int, cId: VD => Long): Graph[VD, ED] = {
      val partitionedEdges = graph.triplets.map({e =>
        val partition = math.abs((cId(e.srcAttr), cId(e.dstAttr)).hashCode()) % numCommunities
        (partition, e)
      }).partitionBy(new HashPartitioner(numCommunities))
        .map{ case (p, e) => Edge(e.srcId, e.dstId, e.attr) }
      Graph(graph.vertices, partitionedEdges)
    }
  }
}
