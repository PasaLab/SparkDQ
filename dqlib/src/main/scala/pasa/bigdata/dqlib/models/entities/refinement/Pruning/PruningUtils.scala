package pasa.bigdata.dqlib.models.entities.refinement.Pruning

object PruningUtils {

  /**
    * Types of weight
    */
  object WeightTypes {
    val CBS = "cbs"
    val chiSquare = "chiSquare"
  }

  /**
    * Types of threshold
    */
  object ThresholdTypes {
    val AVG = "avg"
    val MAX_FRACT_2 = "maxdiv2"
  }

  /**
    * Types of comparison
    */
  object ComparisonTypes {
    val AND = "and"
    val OR = "or"
  }

  /**
    * get all neighbors from other datasets
    * @param profileID profileID
    * @param block block with profiles separated by data source
    * @param separators separator profileIDs
    * @return
    */
  def getAllNeighbors(profileID: Long, block: Array[Set[Long]], separators: Array[Long]): Set[Long] = {
    var output: Set[Long] = Set.empty[Long]
    var i = 0
    while (i < separators.length && profileID > separators(i)) {
      output ++= block(i)
      i += 1
    }
    i += 1
    while (i < separators.length) {
      output ++= block(i)
      i += 1
    }
    if (profileID <= separators.last) {
      output ++= block.last
    }
    output
  }

}
