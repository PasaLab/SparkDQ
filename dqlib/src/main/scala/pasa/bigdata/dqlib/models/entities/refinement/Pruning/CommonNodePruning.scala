package pasa.bigdata.dqlib.models.entities.refinement.Pruning

import org.apache.spark.broadcast.Broadcast

import pasa.bigdata.dqlib.models.entities.structures.ProfileBlocks


object CommonNodePruning {

  /**
    * compute CBS(Common blocks schema) for each profile
    * @param profileBlocks      profile blocks
    * @param blockProfilesMap   block profiles map
    * @param useEntropy         if use entropy
    * @param blockEntropyMap    block entropy map
    * @param weights            weights for each neighbor profile
    * @param entropies          entropies for each neighbor profile
    * @param firstStep          if true all edges will be computed, otherwise only edges that have profileID larger than
    *                           the current profileID, for reusing this function
    * @return
    */
  def calcCBS(profileBlocks: ProfileBlocks,
              blockProfilesMap: Broadcast[scala.collection.Map[Long, Array[Set[Long]]]],
              useEntropy: Boolean,
              blockEntropyMap: Broadcast[scala.collection.Map[Long, Double]],
              weights: Array[Double],
              entropies: Array[Double],
              neighbors: Array[Int],
              firstStep: Boolean
             ): Int = {
    var neighborsNum = 0
    val profileID = profileBlocks.profileID
    val blocks = profileBlocks.blocks
    blocks.foreach { block =>
      val blockID = block.blockID
      val profiles = blockProfilesMap.value.get(blockID)
      if (profiles.isDefined) {
        val profileIDs = profiles.get.head
        val blockEntropy = {
          if (useEntropy) {
            val e = blockEntropyMap.value.get(blockID)
            if (e.isDefined) {
              e.get
            } else {
              0.0
            }
          } else {
            0.0
          }
        }
        profileIDs.foreach { secondProfileID =>
          val neighborID = secondProfileID.toInt
          // small ID to big ID
          if ((neighborID > profileID) || firstStep) {
            // if not first step, just consider neighbors with edge that firstProfileID < secondProfileID
            weights.update(neighborID, weights(neighborID) + 1)
          }
          if (useEntropy) {
            entropies.update(neighborID, entropies(neighborID) + blockEntropy)
          }

          if (weights(neighborID) == 1) {
            neighbors.update(neighborsNum, neighborID)
            neighborsNum += 1
          }
        }
      }
    }
    neighborsNum
  }

  /**
    * compute weights for each neighbor
    * @param profileBlocks            profileBlocks
    * @param weights                  cbs for each profile
    * @param neighbors                neighbors
    * @param entropies                entropy for each profile
    * @param neighborsNum             number of neighbors
    * @param blockProfilesMap         block profiles map
    * @param weightType               weight type
    * @param profileBlocksNumMap      profile and blocks number map
    * @param useEntropy               if use entropy
    */
  def calcWeights(profileBlocks: ProfileBlocks,
                  weights: Array[Double],
                  neighbors: Array[Int],
                  entropies: Array[Double],
                  neighborsNum: Int,
                  blockProfilesMap: Broadcast[scala.collection.Map[Long, Array[Set[Long]]]],
                  weightType: String,
                  profileBlocksNumMap: Broadcast[scala.collection.Map[Long, Int]],
                  useEntropy: Boolean): Unit = {
    if (weightType == PruningUtils.WeightTypes.chiSquare) {
      val profileNumBlocks = profileBlocks.blocks.size
      val totalNumBlocks = blockProfilesMap.value.size.toDouble
      for (i <- 0 until neighborsNum) {
        val neighborID = neighbors(i)
        if (useEntropy) {
          weights.update(neighborID, calcChiSquare(weights(neighborID), profileBlocksNumMap.value(neighborID),
            profileNumBlocks, totalNumBlocks) * entropies(neighborID))
        } else {
          weights.update(neighborID, calcChiSquare(weights(neighborID), profileBlocksNumMap.value(neighborID),
            profileNumBlocks, totalNumBlocks))
        }
      }
    }
  }

  /**
    * compute the chi square
    * @param cbs                common blocks
    * @param neighborNumBlocks  number of blocks that neighbor profile owns
    * @param currentNumBlocks   number of blocks that current profile owns
    * @param totalNumBlocks     total number of blocks
    * @return
    */
  def calcChiSquare(cbs: Double, neighborNumBlocks: Double, currentNumBlocks: Double, totalNumBlocks: Double)
  : Double = {
    val CMat = Array.ofDim[Double](3, 3)
    var weight: Double = 0
    var expectedValue: Double = 0

    CMat(0)(0) = cbs
    CMat(0)(1) = neighborNumBlocks - cbs
    CMat(0)(2) = neighborNumBlocks

    CMat(1)(0) = currentNumBlocks - cbs
    CMat(1)(1) = totalNumBlocks - (neighborNumBlocks + currentNumBlocks - cbs)
    CMat(1)(2) = totalNumBlocks - neighborNumBlocks

    CMat(2)(0) = currentNumBlocks
    CMat(2)(1) = totalNumBlocks - currentNumBlocks

    for (i <- 0 to 1) {
      for (j <- 0 to 1) {
        expectedValue = (CMat(i)(2) * CMat(2)(j)) / totalNumBlocks
        weight += Math.pow(CMat(i)(j) - expectedValue, 2) / expectedValue
      }
    }
    weight
  }

  /**
    * reset the arrays
    * @param weights          weights array
    * @param neighbors        neighbors
    * @param entropies        entropies array
    * @param useEntropy       if use entropy
    * @param neighborsNum     number of neighbors
    */
  def doReset(weights: Array[Double],
              neighbors: Array[Int],
              entropies: Array[Double],
              useEntropy: Boolean,
              neighborsNum: Int): Unit = {
    for (i <- 0 until neighborsNum) {
      val neighborID = neighbors(i)
      weights.update(neighborID, 0)
      if (useEntropy) {
        entropies.update(neighborID, 0)
      }
    }
  }

}
