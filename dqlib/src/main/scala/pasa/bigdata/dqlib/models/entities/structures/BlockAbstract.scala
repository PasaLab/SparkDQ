package pasa.bigdata.dqlib.models.entities.structures

trait BlockAbstract extends Ordered[BlockAbstract] {

  val blockingKey: String         // token value + cluster ID
  val blockID: Long
  var entropy: Double             // cluster entropy
  var clusterID: Integer          // attribute cluster
  val profiles: Array[Set[Long]]  // two respectively clean set of profiles if BlockClean,
                                  // one dirty set of profiles if BlockDirty

  def size: Double = profiles.map(_.size.toDouble).sum
  def getComparisonSize: Double
  def getAllProfiles: Array[Long] = profiles.flatten
  def getComparisons: Set[(Long, Long)]

  // sorted by comparisonSize
  override def compare(that: BlockAbstract): Int = {
    this.getComparisonSize compare that.getComparisonSize
  }

}
