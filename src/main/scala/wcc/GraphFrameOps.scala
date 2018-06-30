package wcc

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.graphframes.GraphFrame
import wcc.GraphXOps.GXOperations

/**
  * Created by tariq on 03/01/18.
  * Adds the SCD algorithm to the GraphFrame class.
  * For documentation refer to [[wcc.DistributedWCC]].
  */
object GraphFrameOps {
  implicit class GFOperations(graph: GraphFrame) {

    def scalableCommunityDetection(spark: SparkSession): DataFrame = {
      val gx = graph.toGraphX.cache()
      val resRDD = gx.scalableCommunityDetection(spark.sparkContext)
      spark.createDataFrame(resRDD, StructType(Seq(
        StructField(name = "id", dataType = LongType, nullable = false),
        StructField(name = "cId", dataType = LongType, nullable = false)
      )))
    }
  }

}
