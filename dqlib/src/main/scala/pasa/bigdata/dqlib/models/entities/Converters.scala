package pasa.bigdata.dqlib.models.entities

import org.apache.spark.rdd.RDD
import pasa.bigdata.dqlib.models.entities.building.TokenBlocking
import pasa.bigdata.dqlib.models.entities.structures._

object Converters {

  /**
    * Transfer block profiles to profile blocks
    * @param blocks   blocks
    * @return
    */
  def blocksToProfileBlocks(blocks: RDD[BlockAbstract]): RDD[ProfileBlocks] = {
    val blocksPerProfile = blocks.flatMap(blockToProfileBlock).groupByKey()
    blocksPerProfile.map {
      case (profileID, blocksWithSize) =>
        ProfileBlocks(profileID, blocksWithSize.toSet)
    }
  }

  /**
    * Block to profile and block with size
    * @param block    block
    * @return
    */
  def blockToProfileBlock(block: BlockAbstract): Iterable[(Long, BlockWithComparisonSize)] = {
    val blockWithComparisonSize = BlockWithComparisonSize(block.blockID, block.getComparisonSize)
    block.getAllProfiles.map((_, blockWithComparisonSize))
  }

  /**
    * Transform profiles block to blocks
    * @param profilesBlock    profiles block
    * @return
    */
  def profilesBlockToBlocks(profilesBlock: RDD[ProfileBlocks]): RDD[BlockAbstract] = {
    val blockAndProfile = profilesBlock.flatMap {
      profileBlocks =>
        val profileID = profileBlocks.profileID
        profileBlocks.blocks.map {
          blockWithSize =>
            (blockWithSize.blockID, profileID)
        }
    }
    val blocks = blockAndProfile.groupByKey().map {
      block =>
        val blockID = block._1
        val profileIDs = block._2.toSet
        BlockDirty(blockID, Array(profileIDs))
    }
    blocks.filter(_.getComparisonSize > 0).map(x => x)
  }


}
