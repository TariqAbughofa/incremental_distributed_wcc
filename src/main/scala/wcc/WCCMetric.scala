package wcc

import org.apache.spark.graphx.{Graph, _}
import scala.reflect.ClassTag

/**
  * Created by tariq on 08/01/18.
  * functions to calculate the Weighted Community Clustering metric
  */
object WCCMetric {

  /**
    * calculates the vertex local WCC in respect to community c
    */
  def computeWccV(vData: VertexData, communityData: CommunityData, vtC: Int, tC: Int): Double = {
    if (vData.t == 0) return 0.0d
    val numerator = tC * vData.vt
    val denominator = vData.t * (communityData.r - 1 + vData.vt - vtC).toDouble
    numerator / denominator
  }

  /**
    * Calculates the approximation of the change to the global wcc that
    * would be caused by inserting this vertex into the new community.
    */
  def computeWccI(cData: CommunityData, dIn: Int, dOut: Int, globalCC: Double, v: Long) = {
    val q = (cData.b - dIn) / cData.r.toDouble
    val t1 = theta1(cData.r, cData.d, dIn, dOut, globalCC, q)
    val t2 = theta2(cData.r, cData.d, globalCC, q)
    val t3 = theta3(cData.r, cData.d, dIn, dOut, globalCC)
    (dIn * t1 + (cData.r - dIn) * t2 + t3) / v.toDouble
  }

  private def theta1(r: Int, d: Double, dIn: Int, dOut: Int, w: Double, q: Double) = {
    val numerator = ((r - 1) * d + 1 + q) * (dIn - 1) * d
    val denominator = (r + q) * ((r - 1) * (r - 2) * math.pow(d, 3) + (dIn - 1) * d + q * (q - 1) * d * w + q * (q - 1) * w + dOut * w)
    numerator / denominator
  }

  private def theta2(r: Int, d: Double, w: Double, q: Double) = {
    val numerator = (r - 1) * (r - 2) * math.pow(d, 3) * ((r - 1) * d + q)
    val denominator = ((r - 1) * (r - 2) * math.pow(d, 3) + q * (q - 1) * w + q * (r - 1) * d * w) * (r + q) * (r - 1 + q)
    - numerator / denominator
  }

  private def theta3(r: Int, d: Double, dIn: Int, dOut: Int, w: Double) = {
    val numerator = dIn * (dIn - 1) * d * (dIn + dOut)
    val denominator = (dIn * (dIn - 1) * d + dOut * (dOut - 1) * w + dOut * dIn * w) * (r + dOut)
    numerator / denominator
  }

}
