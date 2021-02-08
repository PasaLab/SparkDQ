package pasa.bigdata.dqlib.models.entities.structures

/**
  * Block with comparison size
  * @param blockID          block ID
  * @param comparisonSize   comparison size
  */
case class BlockWithComparisonSize(blockID: Long, comparisonSize: Double) extends Ordered[BlockWithComparisonSize] {

  override def compare(that: BlockWithComparisonSize): Int = {
    that.comparisonSize compare this.comparisonSize
  }

}
