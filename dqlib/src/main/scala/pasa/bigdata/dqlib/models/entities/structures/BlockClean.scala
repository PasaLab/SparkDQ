package pasa.bigdata.dqlib.models.entities.structures

case class BlockClean(blockID: Long, profiles: Array[Set[Long]], var entropy: Double = -1, var clusterID: Integer = -1,
                      blockingKey: String = "") extends BlockAbstract with Serializable {

  override def getComparisonSize(): Double = {
    val nonEmptyProfiles = profiles.filter(_.nonEmpty)
    if (nonEmptyProfiles.length > 1) {
      var comparisons: Double = 0
      for (i <- nonEmptyProfiles.indices) {
        for (j <- (i + 1) until nonEmptyProfiles.length) {
          comparisons += nonEmptyProfiles(i).size.toDouble * nonEmptyProfiles(j).size.toDouble
        }
      }
      comparisons
    } else {
      0
    }
  }

  override def getComparisons(): Set[(Long, Long)] = {
    var out: List[(Long, Long)] = Nil
    for (i <- profiles.indices) {
      for (j <- (i + 1) until profiles.length) {
        val a = profiles(i)
        val b = profiles(j)
        for (e1 <- a; e2 <- b) {
          if (e1 < e2) {
            out = (e1, e2) :: out
          } else {
            out = (e2, e1) :: out
          }
        }
      }
    }
    out.toSet
  }
}
