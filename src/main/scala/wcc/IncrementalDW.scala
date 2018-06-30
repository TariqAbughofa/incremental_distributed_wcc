package wcc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx._

import scala.collection.mutable
import scala.reflect.ClassTag
import scalaz.Scalaz._
import wcc.GraphXOps.GXOperations

/**
  * Created by tariq on 07/01/18.
  *
 */
object IncrementalDW {

  private var numPartitions = 200
  private var globalCC = 0.0
  private var vertexCount = 0L

  def run[ED: ClassTag](itGraph: Graph[VertexData, ED], itCStats: Map[VertexId, CommunityData],
                        newEs: EdgeRDD[ED], sc: SparkContext) = {

    val before = System.currentTimeMillis()

    Logger.getRootLogger.warn("Phase: Merging starts...")
    val (fullGraph, mergeTime, newVertices, borderVertices) = merge(itGraph, itCStats, newEs, sc)

    Logger.getRootLogger.warn("Phase: Initial Partition starts...")
    val before2 = System.currentTimeMillis()
    val initGraph = performInitialPartition(fullGraph, newVertices, borderVertices)
    initGraph.vertices.count()
    Logger.getRootLogger.warn(s"Initial Partition Vertex Data took: ${System.currentTimeMillis() - before2}")

    Logger.getRootLogger.warn("Phase: Partition Improvement starts...")
    val (finalGraph, finalCommunityStats) = refinePartition(initGraph, newVertices, borderVertices, sc)

    Logger.getRootLogger.warn(s"incremental took ${"%.3f".format((System.currentTimeMillis() - before - mergeTime) / 1000.0)} seconds")

    val bestWcc = computeGlobalWcc(finalGraph, finalCommunityStats)
    Logger.getRootLogger.warn(s"Best WCC ${"%.3f".format(bestWcc)}")

    val dataGraph = fullGraph.outerJoinVertices(finalGraph.vertices)((vId, _, vDataOpt) =>
      vDataOpt.getOrElse(new VertexData(vId, 0, 0))
    )

    Logger.getRootLogger.warn("STATS FOR FULL...")
    DistributedWCC.run(fullGraph, sc)

    (dataGraph, finalCommunityStats.value)
  }

  def prepare[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], sc: SparkContext) = {
    Logger.getRootLogger.setLevel(Level.ERROR)
    val (communityGraph, cStats) = DistributedWCC.runWithStats(graph, sc)
    Logger.getRootLogger.setLevel(Level.WARN)
    (communityGraph, cStats)
  }

  def merge[ED: ClassTag](itGraph: Graph[VertexData, ED], itCStats: Map[VertexId, CommunityData],
                          newEs: EdgeRDD[ED], sc: SparkContext) = {
    var before = System.currentTimeMillis()

    val batchVertices = Graph.fromEdges(newEs, -1).vertices.mapValues((vId, _) => new VertexData(vId, 0, 0))
    val bVertices = sc.broadcast(batchVertices.collectAsMap())
    val borderVertices = sc.broadcast(itGraph.vertices.filter { case (vId, vData) => bVertices.value.contains(vId) }.collectAsMap())
    val newVs = batchVertices.filter{ case (vId, vData) => !borderVertices.value.contains(vId) }
    val newVertices = sc.broadcast(newVs.collectAsMap())

    Logger.getRootLogger.warn(s"New graph has ${borderVertices.value.size} border vertices " +
      s"and ${newVs.count} new vertices")

    val fg = Graph(itGraph.vertices.union(newVs), itGraph.edges.union(newEs)).cache()
    fg.vertices.count; fg.edges.count
    val mergeTime = System.currentTimeMillis() - before

    before = System.currentTimeMillis()
    val fullGraph = updateVertexData(fg, borderVertices, newVertices)
    vertexCount = fullGraph.vertices.count()
    Logger.getRootLogger.warn(s"Updating Vertex Data took: ${System.currentTimeMillis() - before}")

    globalCC = fullGraph.vertices.map { case (vId, vData) => vData.cc }.sum / vertexCount
    Logger.getRootLogger.warn(s"Global CC $globalCC")

    (fullGraph, mergeTime, newVertices, borderVertices)
  }

  def refinePartition[ED: ClassTag](graph: Graph[VertexData, ED],
                                    newVertices:Broadcast[scala.collection.Map[VertexId, VertexData]],
                                    borderVertices: Broadcast[scala.collection.Map[VertexId, VertexData]],
                                    sc: SparkContext) = {

    val gcc = this.globalCC
    val vc = this.vertexCount
    var bestCs = sc.broadcast(computeCommunityStats(graph))
    var movementGraph = graph

    var it = 0
    do {
      it += 1

      val cvDegrees = movementGraph.aggregateMessages[Map[VertexId, Int]](ctx => {
        ctx.sendToDst(Map(ctx.srcAttr.cId -> 1))
        ctx.sendToSrc(Map(ctx.dstAttr.cId -> 1))
      }, _ |+| _)

      val bestGraph = movementGraph.outerJoinVertices(cvDegrees)((vId, vertex, cDegrees) => {
        val newVertex = vertex.copy()
        newVertex.changed = false
        val candidates = cDegrees.get.keys.map(cId => (cId, bestCs.value(cId))).toMap
        bestMovement(newVertex, cDegrees.get, bestCs.value(vertex.cId), candidates, gcc, vc)
      }).cache()
      bestGraph.vertices.count()
      movementGraph = bestGraph

      bestCs = sc.broadcast(computeCommunityStats(movementGraph))

//        val bestGraph = movementGraph.outerJoinVertices(vChangedNeighbors)((vId, vertex, changedNeighbors) => {
//          val newVertex = vertex.copy()
//          newVertex.changed = false
//          if (changedNeighbors.isDefined) newVertex.cDegrees = updateVertexDegrees(newVertex, changedNeighbors.get)
//          val candidates = newVertex.cDegrees.keys.map(cId => (cId, bestCs.value(cId))).toMap
//          bestMovement(newVertex, bestCs.value(newVertex.cId), candidates, gcc, vc)
//        }).cache()
//        bestGraph.vertices.count()
//        movementGraph = bestGraph

    } while (it <= 5)

    val finalGraph = movementGraph.mapVertices((vId, vData) => {
      val data = vData.copy()
      data.changed = false
      data
    }).partitionByCommunity(numPartitions, _.cId).cache()

    finalGraph.vertices.count()

    (finalGraph, bestCs)
  }

  def printStats[ED: ClassTag](graph: Graph[VertexData, ED], bCommunityStats: Broadcast[Map[VertexId, CommunityData]]) = {
    val wcc = computeGlobalWcc(graph, bCommunityStats)
    Logger.getRootLogger.warn(s"Global WCC: ${"%.3f".format(wcc)}")
    DistributedWCC.printStats(graph.vertices.mapValues((vId, vData) => vData.cId))
  }

  def updateVertexData[ED: ClassTag](graph: Graph[VertexData, ED],
                                     borderVertices: Broadcast[scala.collection.Map[VertexId, VertexData]],
                                     newVertices: Broadcast[scala.collection.Map[VertexId, VertexData]]
                                    ) = {
    val newGraphNeighbors = graph.aggregateMessages[Array[VertexId]](ctx => {
      if (borderVertices.value.contains(ctx.srcId) || newVertices.value.contains(ctx.srcId)) ctx.sendToSrc(Array(ctx.dstId))
      if (borderVertices.value.contains(ctx.dstId) || newVertices.value.contains(ctx.dstId)) ctx.sendToDst(Array(ctx.srcId))
    }, _++_, TripletFields.None)
    val neighborGraph = graph.outerJoinVertices(newGraphNeighbors)((vId, vData, neighbours) => {
      neighbours.getOrElse(Array[VertexId]())
    })
    val borderStats = neighborGraph.aggregateMessages[(Int,Int)](ctx => {
      val borderEdge = borderVertices.value.contains(ctx.srcId) && borderVertices.value.contains(ctx.dstId)
      val newEdge = newVertices.value.contains(ctx.srcId) || newVertices.value.contains(ctx.dstId)
      if (newEdge || borderEdge) {
        val (smallSet, largeSet) = if (ctx.srcAttr.length < ctx.dstAttr.length) {
          (ctx.srcAttr.toSet, ctx.dstAttr.toSet)
        } else {
          (ctx.dstAttr.toSet, ctx.srcAttr.toSet)
        }
        var newVt = true
        val counter = smallSet.foldLeft(0)((c, vId) => {
          if (vId != ctx.srcId && vId != ctx.dstId && largeSet.contains(vId)) {
            if (newEdge || newVertices.value.contains(vId)) {
              c + 1
            } else {
              newVt = false
              c
            }
          } else {
            c
          }
        })
        val i = if (counter > 0 && newVt) 1 else 0
        ctx.sendToSrc((counter, i))
        ctx.sendToDst((counter, i))
      }
    }, (e1,e2) => (e1._1 + e2._1, e1._2 + e2._2))

    graph.outerJoinVertices(borderStats)((vId, vData, statsOpt) => {
      if (statsOpt.isDefined) {
        val (t, vt) = if (borderVertices.value.contains(vId)) {
          (vData.t + statsOpt.get._1 / 2, vData.vt + statsOpt.get._2)
        } else {
          (statsOpt.get._1 / 2, statsOpt.get._2)
        }
        new VertexData(vId, t, vt)
      } else {
        vData
      }
    })
  }

//  def updateVertexDegrees(vertex: VertexData, changedNeighbors: Array[(Long, Long)]) = {
//    val degrees = mutable.Map(vertex.cDegrees.toSeq: _*)
//    changedNeighbors.foreach{ case (cId, prevCId) =>
//      val prev = vertex.cDegrees(prevCId)
//      if (prev <= 1) {
//        degrees.remove(prevCId)
//      } else {
//        degrees.update(prevCId, prev - 1)
//      }
//      degrees.update(cId, vertex.cDegrees.getOrElse(cId, 0) + 1)
//    }
//    Map(degrees.toSeq: _*)
//  }

  private def bestMovement(vertex: VertexData, cDegrees: Map[VertexId, Int],
                           community: CommunityData,
                           candidates: Map[VertexId, CommunityData],
                           globalCC: Double, vertexCount: Long): VertexData = {

    val wccR = computeWccR(vertex, cDegrees, community, globalCC, vertexCount)
    var wccT = 0.0d
    var bestC = vertex.cId

    candidates.foreach { case (cId, cData) =>
      if (vertex.cId != cId && cData.r > 1) {
        val dIn = cDegrees(cId)
        val dOut = cDegrees.values.sum - dIn
        val candidateWccT = wccR + WCCMetric.computeWccI(cData, dIn, dOut, globalCC, vertexCount)
        if (candidateWccT > wccT) {
          wccT = candidateWccT
          bestC = cId
        }
      }
    }

    // m ← [REMOVE];
    if (wccR - wccT > 0.00001 && wccR > 0.0d) {
      vertex.cId = vertex.vId
    }
    // m ← [TRANSFER , bestC];
    else if (wccT > 0.0d) {
      vertex.cId = bestC
    }
    // m ← [STAY];

    vertex
  }

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

  private def performInitialPartition[ED: ClassTag](graph: Graph[VertexData, ED],
                                                   newVertices:Broadcast[scala.collection.Map[VertexId, VertexData]],
                                                   borderVertices: Broadcast[scala.collection.Map[VertexId, VertexData]]) = {

    val initGraph = graph.outerJoinVertices(graph.collectNeighborIds(EdgeDirection.Either))((vId, vData, neighbors) => {
      (vData, neighbors.getOrElse(Array[VertexId]()))
    }).subgraph(t => {
      t.srcAttr._2.intersect(t.dstAttr._2).nonEmpty
    }, (vertexId, vData) => {
      vData._1.t > 0
    }).mapVertices((vId, vData) => vData._1)

    val pregelGraph = initGraph.pregel[Map[Long, VertexMessage]](Map.empty[Long, VertexMessage])(
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
        val messages = mutable.Map.empty[Long, Map[Long, VertexMessage]]
        val (to, from) = t.srcAttr.compareTo(t.dstAttr)
        if (from.changed && (newVertices.value.contains(to.vId) || borderVertices.value.contains(to.vId))) {
          // node changed its community
          // broadcast to lower neighbors and notify self to stop sending messages
          // in case it is the highest among its neighbors.
          val msg = VertexMessage.create(from)
          messages.put(from.vId, Map[Long, VertexMessage]((from.vId, msg)))
          messages.put(to.vId, Map[Long, VertexMessage]((from.vId, msg)))
        }
        messages.toIterator
      }, mergeMsg = _ ++ _)

    pregelGraph.mapVertices((vId, data) => {
      data.changed = true
      data.neighbors = List.empty
      data
    }).partitionByCommunity(numPartitions, _.cId)
  }

  private def updateNeighborsCommunities(vertexData: VertexData, neighbors: Map[Long,VertexMessage]) = {
    vertexData.neighbors.map(vData => {
      vData.cId = neighbors.getOrElse(vData.vId, vData).cId
      vData
    })
  }

  private def getHighestCenterNeighbor(neighbors: List[VertexMessage]) = {
    neighbors.filter(_.isCenter).sorted(VertexMessage.ordering.reverse).headOption
  }

  private def computeGlobalWcc[ED: ClassTag](graph: Graph[VertexData, ED], bCommunityStats: Broadcast[Map[VertexId, CommunityData]]) = {
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

  private def computeWccR(vertex: VertexData, cDegrees: Map[VertexId, Int],
                          cData: CommunityData, globalCC: Double, vertexCount: Long): Double = {
    // if vertex is isolated
    if (cData.r == 1) return 0.0d
    val dIn = cDegrees.getOrElse(vertex.cId, 0)
    val dOut = cDegrees.values.sum - dIn
    val cDataWithVertexRemoved = new CommunityData(
      cData.r - 1,
      cData.a - dIn,
      cData.b + dIn - dOut
    )
    - WCCMetric.computeWccI(cDataWithVertexRemoved, dIn, dOut, globalCC, vertexCount)
  }

}
