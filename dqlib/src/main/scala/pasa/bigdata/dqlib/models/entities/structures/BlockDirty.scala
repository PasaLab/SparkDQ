package pasa.bigdata.dqlib.models.entities.structures

case class BlockDirty(blockID: Long, profiles: Array[Set[Long]], var entropy: Double = -1, var clusterID: Integer = -1,
                      blockingKey: String = "") extends BlockAbstract with Serializable {

  override def getComparisonSize: Double = {
    val len = profiles.head.size.toDouble
    len * (len - 1)
  }

  override def getComparisons: Set[(Long, Long)] = {
    profiles.head.toList.combinations(2).map { x =>
      if (x.head < x.last) {
        (x.head, x.last)
      } else {
        (x.last, x.head)
      }
    }.toSet
  }

}
