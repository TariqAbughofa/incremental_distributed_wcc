package wcc

import org.apache.spark.graphx.VertexId

/**
  * Created by tariq on 01/01/18.
  *
  * @param vId the vertex Identifier.
  * @param t the number of links between neighbors of the vertex aka triangle count.
  * @param vt The number of vertices that form at least one triangle with x.
  */
class VertexData(val vId: VertexId = -1L, val t: Int = 0, val vt: Int = 0) extends Serializable {

  var cId: VertexId = vId

  def cc: Double = {
    if (vt < 2) {
      0.0
    } else {
      2.0 * t / (vt * (vt - 1))
    }
  }

  var changed: Boolean = false

  var neighbors: List[VertexMessage] = List.empty

  def copy(): VertexData = {
    val v = new VertexData(this.vId, this.t, this.vt)
    v.cId = this.cId
    v.changed = this.changed
    v.neighbors = this.neighbors
    v
  }

  def isCenter = {
    this.vId == this.cId
  }

  def compareTo(vs: VertexData) = {
    if (VertexData.ordering.lt(this, vs)) {
      (this, vs)
    } else {
      (vs, this)
    }
  }

}

object VertexData {
  implicit val ordering: Ordering[VertexData] = Ordering.by({ data: VertexData =>
    (data.cc, data.vt, data.vId)
  })
}
