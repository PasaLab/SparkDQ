package pasa.bigdata.dqlib.models.entities.structures

/**
  * Profile and blocks containing it with comparison information
  * @param profileID      profile ID
  * @param blocks         blocks containing this profile
  */
case class ProfileBlocks(profileID: Long, blocks: Set[BlockWithComparisonSize]) extends Serializable {
}
