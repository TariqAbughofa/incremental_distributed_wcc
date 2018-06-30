package wcc

import org.apache.log4j.Logger

import scala.reflect.ClassTag
import scala.collection.mutable
import scalaz.Scalaz._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{VertexRDD, _}
import org.apache.spark.graphx.lib.TriangleCount
import wcc.GraphXOps.GXOperations
/**
  * Created by tariq on 31/12/17.
  * Implementation of the "Scalable Community Detection" algorithm.
  * @note We always cache a new graph if it will be used multiple time (and un-cache
  * it afterwards).
  * @see "High quality, scalable and parallel community detection for large real graphs"
  *      https://dl.acm.org/citation.cfm?id=2568010
  * @see "Distributed Community Detection with the WCC Metric"
  *      https://dl.acm.org/citation.cfm?id=2744715
  * @see "Shaping communities out of triangles"
  *      https://dl.acm.org/citation.cfm?id=2398496
  */
object DistributedWCC {

  private val threshold = 0.01f

  private var maxRetries = 5
  private var numPartitions = 200
  var vertexCount = 0L

  def runWithStats[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], sc: SparkContext,
                                      maxRetries: Int = this.maxRetries,
                                      partitions: Int = this.numPartitions,
                                      isCanonical: Boolean = false
                                     ): (Graph[VertexData, ED], Map[VertexId, CommunityData]) = {
    require(maxRetries >= 0, s"Number of iterations must be greater than or equal to 0," +
      s" but got $maxRetries")
    require(partitions > 0 , s"Number of partitions must be greater than 0," +
      s" but got $partitions")

    this.maxRetries = maxRetries
    this.numPartitions = partitions
    this.vertexCount = graph.vertices.count

    val before = System.currentTimeMillis()

    val optimizedGraph = preprocess(graph, isCanonical)
    val initGraph = performInitialPartition(optimizedGraph)
    val (communityGraph, cStats) = refinePartition(initGraph, sc)

    Logger.getRootLogger.warn(s"Took ${"%.3f".format((System.currentTimeMillis() - before) / 1000.0)} secconds.")

    val dataGraph = graph.outerJoinVertices(communityGraph.vertices)((vId, _, vDataOpt) =>
      vDataOpt.getOrElse(new VertexData(vId, 0, 0))
    )

    (dataGraph, cStats)
  }

  /**
    *
    * @param graph the graph on which to partition into communities
    * @param sc the current spark context
    * @param maxRetries the number of iterations to run
    * @param partitions the number of partitions
    * @tparam VD the original vertex attribute
    * @tparam ED the original edge attribute
    * @return
    */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], sc: SparkContext,
                                      maxRetries: Int = this.maxRetries,
                                      partitions: Int = this.numPartitions,
                                      isCanonical: Boolean = false
                                     ) = {
    require(maxRetries >= 0, s"Number of iterations must be greater than or equal to 0," +
      s" but got $maxRetries")
    require(partitions > 0 , s"Number of partitions must be greater than 0," +
      s" but got $partitions")

    this.maxRetries = maxRetries
    this.numPartitions = partitions
    this.vertexCount = graph.vertices.count

    val before = System.currentTimeMillis()

    Logger.getRootLogger.warn("Phase: Preprocessing Start...")
    val optimizedGraph = preprocess(graph, isCanonical)

    Logger.getRootLogger.warn("Phase: Community Initialization Start...")
    val initGraph = performInitialPartition(optimizedGraph)
    val initCommunityMap = initGraph.vertices.mapValues((vId, vData) => vData.cId)
//    printStats(initCommunityMap)

    Logger.getRootLogger.warn("Phase: WCC Iteration Start...")
    val communityMap = refinePartition(initGraph, sc)._1.vertices.mapValues((vId, vData) => vData.cId)

    Logger.getRootLogger.warn(s"Took ${"%.3f".format((System.currentTimeMillis() - before) / 1000.0)} secconds.")

    printStats(communityMap)

    communityMap
  }

  def printStats(communityMap: VertexRDD[VertexId]) = {
    Logger.getRootLogger.warn(s"Generated ${communityMap.values.distinct.count()} communities.")
    val majorCommunityStats = communityMap.map(x => (x._2, 1L)).reduceByKey(_ + _).filter(_._2 > 1)
    Logger.getRootLogger.warn(s"Generated ${majorCommunityStats.count()} major communities.")
    majorCommunityStats.sortBy(_._2, ascending = false).take(10).foreach(println)
  }

  /**
    * PHASE I
    * Optimize the graph by removing edges that does not close any triangles:
    * - Compute the number of triangles passing through each vertex.
    * - Get the neighbors of each vertex.
    * - Remove edges/vertices that are not part of any triangles.
    * @param graph
    * @param isCanonical whether the passed `graph` is canonical or not:
    * The Triangle Count algorithm requires that the graph to be canonical. However,
    * The canonicalization procedure is costly as it requires repartitioning the graph.
    * The parameter `isCanonical` should be passed as true if the input data is
    * already in "canonical form", meaning all of the following holds:
    * <ul>
    * <li> There are no self edges</li>
    * <li> All edges are oriented (src is greater than dst)</li>
    * <li> There are no duplicate edges</li>
    * </ul>
    */
  def preprocess[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], isCanonical: Boolean): Graph[VertexData, ED] = {
    Logger.getRootLogger.warn("Phase: Preprocessing - Counting Triangles")
    var before = System.currentTimeMillis()
    val tcGraph = if (isCanonical) {
      TriangleCount.runPreCanonicalized(graph)
    } else {
      TriangleCount.run(graph)
    }
    tcGraph.vertices.count
    Logger.getRootLogger.warn(s"Counting Triangles took: ${System.currentTimeMillis() - before}")

    // Filter by:
    // 1 - Removing vertices that does not belong to any triangles.
    // 2 - Removing edges with removed source/destination vertices.
    // 3 - Removing edges that are not part of triangles.
    Logger.getRootLogger.warn("Phase: Preprocessing - Graph Optimization")
    before = System.currentTimeMillis()
    val neighborRDD = graph.collectNeighborIds(EdgeDirection.Either)
    val subGraph = tcGraph.outerJoinVertices(neighborRDD)((vertexId, triangleCount, neighbors) => {
      (triangleCount, neighbors.getOrElse(Array[VertexId]()))
    }).subgraph(t => {
      t.srcAttr._2.intersect(t.dstAttr._2).nonEmpty
    }, (vertexId, vData) => {
      vData._1 > 0
    })//.partitionBy(PartitionStrategy.EdgePartition2D).cache()

    Logger.getRootLogger.warn(s"vertices: ${subGraph.vertices.count}, edges: ${subGraph.edges.count}")
    Logger.getRootLogger.warn(s"Optimization took: ${System.currentTimeMillis() - before}")

    Logger.getRootLogger.warn("Phase: Preprocessing - Saving Vertices Data")
    before = System.currentTimeMillis()
    val optGraph = subGraph.outerJoinVertices(subGraph.degrees)((vertexId, vData, degree) => {
      new VertexData(vertexId, vData._1, degree.getOrElse(0))
    })//.partitionBy(PartitionStrategy.EdgePartition2D)

    optGraph.vertices.count()
    optGraph.edges.count()
    Logger.getRootLogger.warn(s"Degree counting took: ${System.currentTimeMillis() - before}")

    subGraph.unpersist(blocking = false)
    optGraph
  }

  /**
    * PHASE II
    * Computes an initial partition of the graph:
    * - Compute the clustering coefficient of each vertex of the graph.
    * - Sort vertices by the clustering coefficient then degree in descending order.
    * - We start in a state in which all vertices are considered as not `visited`.
    * - For each non-`visited` vertex, in the calculated order, do the following:
    *     - Create a new community that contains the vertex and all its neighbors
    *       that we did not visit so far
    *     - Mark the vertex and its neighbors as `visited`.
    * - The partition contains all the created communities.
    * @param graph
    */
  def performInitialPartition[ED: ClassTag](graph: Graph[VertexData, ED]): Graph[VertexData, ED] = {
    graph.cache()
    val before = System.currentTimeMillis()

    val pregelGraph = graph.pregel(Map.empty[Long, VertexMessage])(
      vprog = (vId: VertexId, data: VertexData, messages: Map[Long, VertexMessage]) => {
      val newData = data.copy()
      if (messages.nonEmpty) {
        newData.changed = false
        // Sender is the highest among its neighbors.
        // Stop broadcasting the change.
        if (messages.tail.isEmpty && messages.head._2.vId == vId) {
          // Do nothing
        } else {

          // update neighbors data.
          newData.neighbors = if (newData.neighbors.isEmpty) {
            (messages - vId).values.toList
          } else {
            updateNeighborsCommunities(newData, messages)
          }

          val highestNeighbor = getHighestCenterNeighbor(newData.neighbors)
          // If the highest neighbor that is a center is higher than us.
          // Receiver becomes a border node of that neighbor community.
          // Broadcast the change only if it's changed from a center to a border
          if (highestNeighbor.isDefined && VertexMessage.ordering.gt(highestNeighbor.get, VertexMessage.create(newData))) {
            newData.changed = newData.isCenter
            newData.cId = highestNeighbor.get.vId
          }
          // If no neighbor is higher than us and receiver is not a center
          // Receiver become a center of its own community.
          // Broadcast the change.
          else {
            newData.changed = !newData.isCenter
            newData.cId = vId
          }
        }
      } else {
        newData.changed = true
      }
      newData
    }, sendMsg = (t: EdgeTriplet[VertexData, ED]) => {
      val messages = mutable.Map[Long, Map[Long, VertexMessage]]()
      val (to, from) = t.srcAttr.compareTo(t.dstAttr)
      if (from.changed) {
        // node changed its community
        // broadcast to lower neighbors and notify self to stop sending messages
        // in case it is the highest among its neighbors.
        val msg = VertexMessage.create(from)
        messages.put(from.vId, Map[Long, VertexMessage]((from.vId, msg)))
        messages.put(to.vId, Map[Long, VertexMessage]((from.vId, msg)))
      }
      messages.toIterator
    }, mergeMsg = _ ++ _)

    pregelGraph.vertices.count()
    Logger.getRootLogger.warn(s"Initial Partition took: ${System.currentTimeMillis() - before}")

    pregelGraph.mapVertices((vId, vData) => {
      val data = vData.copy()
      data.changed = false
      data.neighbors = List.empty
      data
    }).partitionByCommunity(numPartitions, _.cId)
  }

  /**
    * PHASE III
    * Improve on the initial partition while having an improvement in WCC that is more
    * than a threshold:
    * - Calculate the "best community movement" for each vertex in the graph.
    * - Calculate the WCC of the new partition.
    * - If the movement improve WCC, apply it to create a new partition.
    * - Repeat the previous steps while the improvement in WCC is greater than the
    *   proposed threshold.
    * @param graph
    * @param sc
    * @tparam ED the original edge attribute
    * @return
    */
  def refinePartition[ED: ClassTag](graph: Graph[VertexData, ED], sc: SparkContext) = {
    graph.cache()

    val globalCC = graph.vertices.map { case (vId, vData) => vData.cc }.sum / vertexCount
    // broadcasts are used to avoid network transfers all the time.
    var bestCs = sc.broadcast(computeCommunityStats(graph))
    var bestPartition = graph
    var bestWcc = computeGlobalWCC(bestPartition, bestCs)
    Logger.getRootLogger.warn(s"Global CC $globalCC")
    Logger.getRootLogger.warn(s"Initial WCC $bestWcc")

    var foundNewBestPartition = true
    var retriesLeft = maxRetries

    do {

      var before = System.currentTimeMillis()
      val movementGraph = getBestMovements(bestPartition, bestCs, globalCC, vertexCount).cache()
      movementGraph.vertices.count()
      Logger.getRootLogger.warn(s"Movement took: ${System.currentTimeMillis() - before}")
      // calculate new global WCC
      before = System.currentTimeMillis()
      val newCs = sc.broadcast(computeCommunityStats(movementGraph))

      val newWcc = computeGlobalWCC(movementGraph, newCs)
      retriesLeft -= 1
      Logger.getRootLogger.warn(s"calculate WCC took: ${System.currentTimeMillis() - before}")

      Logger.getRootLogger.warn(s"New WCC ${"%.3f".format(newWcc)}")
      Logger.getRootLogger.warn(s"Retries left $retriesLeft")

      // if the movements improve WCC apply them
      if (newWcc > bestWcc) {
        if (newWcc / bestWcc - 1 > threshold) {
          Logger.getRootLogger.warn("Resetting retries.")
          retriesLeft = maxRetries
        }
        bestPartition.unpersist(blocking = false)
        bestPartition = movementGraph.partitionByCommunity(numPartitions, _.cId).cache()
        bestWcc = newWcc
        bestCs = newCs
        bestPartition.vertices.count()
        movementGraph.unpersist(blocking = false)
      } else {
        foundNewBestPartition = false
      }

    } while (foundNewBestPartition && retriesLeft > 0)

    Logger.getRootLogger.warn(s"Best WCC ${"%.3f".format(bestWcc)}")

    (bestPartition, bestCs.value)
  }

  /**
    * reflect changes to neighbors communities.
    * @param vertexData
    * @param neighbors
    * @return
    */
  private def updateNeighborsCommunities(vertexData: VertexData, neighbors: Map[Long,VertexMessage]) = {
    vertexData.neighbors.map(vData => {
      vData.cId = neighbors.getOrElse(vData.vId, vData).cId
      vData
    })
  }

  /**
    * get the neighbor how is the highest and a center of its own community.
    * @param neighbors
    * @return
    */
  private def getHighestCenterNeighbor(neighbors: List[VertexMessage]) = {
    neighbors.filter(_.isCenter).sorted(VertexMessage.ordering.reverse).headOption
  }

  /**
    * Gather some community statistics:
    * As they are defined in `CommunityData`
    * @param graph
    * @return a map of communities and their statistics
    */
  private def computeCommunityStats[ED: ClassTag](graph: Graph[VertexData, ED]): Map[VertexId, CommunityData] = {
    val communitySizes = graph.vertices.map({ case (vId, vData) =>
      (vData.cId, 1)
    }).reduceByKey(_+_).collectAsMap()

    val communityEdges = graph.triplets.flatMap({ triplet =>
      if(triplet.srcAttr.cId == triplet.dstAttr.cId){
        Iterator((("INT", triplet.srcAttr.cId), 1))
      } else {
        Iterator((("EXT", triplet.srcAttr.cId), 1), (("EXT", triplet.dstAttr.cId), 1))
      }
    }).reduceByKey(_+_).collectAsMap()

    communitySizes.map({ case (community, size) =>
      val intEdges = communityEdges.getOrElse(("INT", community), 0)
      val extEdges = communityEdges.getOrElse(("EXT", community), 0)
      (community, new CommunityData(size, intEdges, extEdges))
    }).toMap
  }

  /**
    * calculates the best movements for all vertices in a partition.
    * @param graph
    * @param bCommunityStats a broadcast of community statistics
    * @param globalCC the global clustering coefficient
    * @param vertexCount the count of vertices in the graph
    * @tparam ED
    * @return
    */
  private def getBestMovements[ED: ClassTag](graph: Graph[VertexData, ED],
                                             bCommunityStats: Broadcast[Map[VertexId, CommunityData]],
                                             globalCC: Double, vertexCount: Long): Graph[VertexData, ED] = {
    val vertexCommunityDegrees = graph.aggregateMessages[Map[VertexId, Int]](ctx => {
      ctx.sendToDst(Map(ctx.srcAttr.cId -> 1))
      ctx.sendToSrc(Map(ctx.dstAttr.cId -> 1))
    }, _ |+| _)

    graph.outerJoinVertices(vertexCommunityDegrees)((vId, vertex, vcDegrees) => {
      bestMovement(vertex, vcDegrees.get, bCommunityStats.value, globalCC, vertexCount)
    })
  }

  /**
    * Implementation of the "bestMovement" algorithm:
    * For each vertex of the graph we choose the movement that improves the WCC of
    * the partition the most. There are three types of possible movements:
    *  - Transfer: The vertex moves from its community to the community of a
    *    neighboring vertex.
    *  - Remove: The vertex removes itself from its current community and becomes
    *    the sole member of its own.
    *  - Stay: The vertex remains in its current community.
    * @param vertex the vertex to move
    * @param vcDegrees map of communities adjacent to vertex `vId` and the count of
    *                 edge connected them to vertex `vId`
    * @param communityStats community statistics
    * @param globalCC the graph average clustering coeficient value
    * @param vertexCount the vertices count
    * @return an updated statistics of vertex `vId`
    */
  private def bestMovement(vertex: VertexData, vcDegrees: Map[VertexId, Int],
                                communityStats: Map[VertexId, CommunityData],
                                globalCC: Double, vertexCount: Long): VertexData = {
    val newVertex = vertex.copy()

    // WCCR(v,C) computes the improvement of the WCC of a partition when
    // a vertex v is removed from community C and placed in its own isolated community.
    val wccR = computeWccR(vertex, vcDegrees, communityStats(vertex.cId), globalCC, vertexCount)
    // WCCT (v,C1,C2) computes the improvement of the WCC of a partition when vertex
    // v is transferred from community C1 and to C2.
    var wccT = 0.0d
    var bestC = vertex.cId
    vcDegrees.foreach { case (cId, dIn) =>
      val cData = communityStats(cId)
      if (vertex.cId != cId && cData.r > 1) {
        val dOut = vcDegrees.values.sum - dIn
        val candidateWccT = wccR + WCCMetric.computeWccI(cData, dIn, dOut, globalCC, vertexCount)
        if (candidateWccT > wccT) {
          wccT = candidateWccT
          bestC = cId
        }
      }
    }

    // m ← [REMOVE];
    if (wccR - wccT > 0.00001 && wccR > 0.0d) {
      newVertex.cId = vertex.vId
    }
    // m ← [TRANSFER , bestC];
    else if (wccT > 0.0d) {
      newVertex.cId = bestC
    }
    // m ← [STAY];

    newVertex
  }

  /**
    * Calculates the approximation of the change to the global wcc that
    * would be caused by removing this vertex from its current community
    * and isolated.
    */
  private def computeWccR(vertex: VertexData, vcDegrees: Map[VertexId, Int],
                               cData: CommunityData, globalCC: Double, vertexCount: Long): Double = {
    // if vertex is isolated
    if (cData.r == 1) return 0.0d
    val dIn = vcDegrees.getOrElse(vertex.cId, 0)
    val dOut = vcDegrees.values.sum - dIn
    val cDataWithVertexRemoved = new CommunityData(
      cData.r - 1,
      cData.a - dIn,
      cData.b + dIn - dOut
    )
    - WCCMetric.computeWccI(cDataWithVertexRemoved, dIn, dOut, globalCC, vertexCount)
  }

  /**
    * Update the graph vertices with their new WCC values in respect to their current communities.
    * @param graph
    * @param bCommunityStats
    * @tparam ED the original edge attribute
    * @return
    */
  private def computeGlobalWCC[ED: ClassTag](graph: Graph[VertexData, ED], bCommunityStats: Broadcast[Map[VertexId, CommunityData]]) = {
    val communityNeighborIds = collectCommunityNeighborIds(graph)
    val communityNeighborGraph = graph.outerJoinVertices(communityNeighborIds)((vId, vData, neighbours) => {
      (vData, neighbours.getOrElse(Array[VertexId]()))
    }).cache()
    val trianglesByCommunity = countCommunityTriangles(communityNeighborGraph)
    communityNeighborGraph.outerJoinVertices(trianglesByCommunity)((vId, data, tC) => {
      WCCMetric.computeWccV(data._1, bCommunityStats.value(data._1.cId), data._2.length, tC.getOrElse(0))
    }).vertices.map { case (vId, wcc) => wcc }.sum / vertexCount
  }

  private def collectCommunityNeighborIds[ED: ClassTag](graph: Graph[VertexData, ED]): VertexRDD[Array[VertexId]] = {
    graph.aggregateMessages(ctx => {
      if (ctx.dstAttr.cId == ctx.srcAttr.cId) {
        ctx.sendToDst(Array(ctx.srcId))
        ctx.sendToSrc(Array(ctx.dstId))
      }
    }, _ ++ _)
  }

  private def countCommunityTriangles[ED: ClassTag](graph: Graph[(VertexData, Array[Long]), ED]) = {
    graph.aggregateMessages[Int](ctx => {
      if (ctx.srcAttr._1.cId == ctx.dstAttr._1.cId) {
        val (smallSet, largeSet) = if (ctx.srcAttr._2.length < ctx.dstAttr._2.length) {
          (ctx.srcAttr._2.toSet, ctx.dstAttr._2.toSet)
        } else {
          (ctx.dstAttr._2.toSet, ctx.srcAttr._2.toSet)
        }
        val counter = smallSet.foldLeft(0)((c, vId) => {
          if (vId != ctx.srcId && vId != ctx.dstId && largeSet.contains(vId)) {
            c + 1
          } else {
            c
          }
        })
        ctx.sendToSrc(counter)
        ctx.sendToDst(counter)
      }
    }, _ + _).mapValues(_/2)
  }

}
