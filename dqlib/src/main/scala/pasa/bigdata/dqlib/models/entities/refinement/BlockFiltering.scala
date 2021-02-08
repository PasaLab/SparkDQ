package pasa.bigdata.dqlib.models.entities.refinement

import org.apache.spark.rdd.RDD
import pasa.bigdata.dqlib.models.entities.structures.ProfileBlocks

object BlockFiltering {

  /**
    * Remove (1-r)*100% blocks for each profile
    * @param profilesWithBlocks   profile with blocks
    * @param r                    keep rate
    * @param minCardinality       minimum cardinality
    * @return
    */
  def blockFiltering(profilesWithBlocks: RDD[ProfileBlocks], r: Double, minCardinality: Int = 1): RDD[ProfileBlocks] = {
    profilesWithBlocks.map {
      profileBlocks =>
        val blocksSortedByComparisons = profileBlocks.blocks.toList.sortBy(_.comparisonSize)
        val blocksToKeep = Math.max(Math.round(blocksSortedByComparisons.size * r).toInt, minCardinality)
        val threshold = blocksSortedByComparisons(blocksToKeep - 1).comparisonSize
        ProfileBlocks(profileBlocks.profileID, blocksSortedByComparisons.filter(_.comparisonSize <= threshold).toSet)
    }
  }

}
