package wcc

/**
  * Created by tariq on 01/05/18.
  *
  * Statistics about a certain community
  * @param r the size of the community (number of vertices).
  * @param a number of internal edges.
  * @param b number of external edges.
  */
class CommunityData(val r: Int, val a: Double, val b: Int) extends Serializable {
  // the edge density δ = 2 * number of edges / squared number of vertices.
  val d = 2 * a / math.pow(r, 2)

  def removeVertex(dIn: Int, dOut: Int) = {
    new CommunityData(
      this.r - 1,
      this.a - dIn,
      this.b + dIn - dOut
    )
  }

  def addVertex(dIn: Int, dOut: Int) = {
    new CommunityData(
      this.r + 1,
      this.a + dIn,
      this.b - dIn + dOut
    )
  }
}

