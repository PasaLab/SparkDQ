package pasa.bigdata.dqlib.models.entities.refinement.Pruning

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import pasa.bigdata.dqlib.models.entities.structures.{ProfileBlocks, UnweightedEdge}

object WNP {

  /**
    * Weight node pruning algorithm
    * @param profileBlocksFiltered      profiles block
    * @param blockProfilesMap           block profiles map
    * @param maxID                      maximum profile ID
    * @param groundTruth                ground truth formatted as (profile1, profile2), for testing precision and recall
    * @param thresholdType              threshold type
    * @param weightType                 weight type
    * @param profileBlocksNumMap        profile and number of blocks map
    * @param useEntropy                 if use entropy
    * @param blockEntropyMap            block entropy map
    * @param chi2divider                used only in the chiSquare weight method to compute the threshold
    * @param comparisonType             comparison type
    * @return
    */
  def WNP(profileBlocksFiltered: RDD[ProfileBlocks],
          blockProfilesMap: Broadcast[scala.collection.Map[Long, Array[Set[Long]]]],
          maxID: Int,
          groundTruth: Broadcast[scala.collection.immutable.Set[(Long, Long)]],
          thresholdType: String = PruningUtils.ThresholdTypes.AVG,
          weightType: String = PruningUtils.WeightTypes.CBS,
          profileBlocksNumMap: Broadcast[scala.collection.Map[Long, Int]] = null,
          useEntropy: Boolean = false,
          blockEntropyMap: Broadcast[scala.collection.Map[Long, Double]] = null,
          chi2divider: Double = 2.0,
          comparisonType: String = PruningUtils.ComparisonTypes.OR
         ): RDD[(Double, Double, Iterable[UnweightedEdge])] = {
    val sc = profileBlocksFiltered.sparkContext
    // calculate thresholds for each profile
    val thresholds = sc.broadcast(calcThresholds(profileBlocksFiltered, blockProfilesMap, maxID, thresholdType,
      weightType, profileBlocksNumMap, useEntropy, blockEntropyMap).collectAsMap())
    val edges = pruning(profileBlocksFiltered, blockProfilesMap, maxID, groundTruth, thresholdType, weightType,
      profileBlocksNumMap, useEntropy, blockEntropyMap, chi2divider, comparisonType, thresholds)
    thresholds.unpersist()
    edges
  }

  /**
    * calculate the threshold for each profile
    * @param profileBlocksFiltered      profile blocks
    * @param blockProfilesMap           block profiles map
    * @param maxID                      maximum profile ID
    * @param thresholdType              threshold type
    * @param weightType                 weight type
    * @param profileBlocksNumMap        profile and number of blocks map
    * @param useEntropy                 if use entropy
    * @param blockEntropyMap            block entropy map
    * @return
    */
  def calcThresholds(profileBlocksFiltered: RDD[ProfileBlocks],
                     blockProfilesMap: Broadcast[scala.collection.Map[Long, Array[Set[Long]]]],
                     maxID: Int,
                     thresholdType: String,
                     weightType: String,
                     profileBlocksNumMap: Broadcast[scala.collection.Map[Long, Int]],
                     useEntropy: Boolean,
                     blockEntropyMap: Broadcast[scala.collection.Map[Long, Double]]
                    ): RDD[(Long, Double)] = {
    profileBlocksFiltered.mapPartitions { partition =>
      val localWeights = Array.fill[Double](maxID + 1) { 0 }
      val entropies: Array[Double] = {
        if (useEntropy) {
          Array.fill[Double](maxID + 1) { 0.0 }
        } else {
          null
        }
      }
      val neighbors = Array.ofDim[Int](maxID + 1)
      var neighborsNum = 0
      partition.map {
        profileBlocks =>
          neighborsNum = CommonNodePruning.calcCBS(profileBlocks, blockProfilesMap, useEntropy, blockEntropyMap,
            localWeights, entropies, neighbors, firstStep = true)
          CommonNodePruning.calcWeights(profileBlocks, localWeights, neighbors, entropies, neighborsNum,
            blockProfilesMap, weightType, profileBlocksNumMap, useEntropy)
          val threshold = calcThreshold(localWeights, neighbors, neighborsNum, PruningUtils.ThresholdTypes.AVG)
          CommonNodePruning.doReset(localWeights, neighbors, entropies, useEntropy, neighborsNum)
          (profileBlocks.profileID, threshold)
      }
    }
  }

  /**
    * compute the threshold of a profile
    * @param weights        weights with each neighbor
    * @param neighbors      neighbors
    * @param neighborsNum   number of neighbors
    * @param thresholdType  threshold type
    * @return
    */
  def calcThreshold(weights: Array[Double],
                    neighbors: Array[Int],
                    neighborsNum: Int,
                    thresholdType: String): Double = {
    var acc: Double = 0
    for (i <- 0 until neighborsNum) {
      val neighborID = neighbors(i)
      if (thresholdType == PruningUtils.ThresholdTypes.AVG) {
        acc += weights(neighborID)
      } else if (thresholdType == PruningUtils.ThresholdTypes.MAX_FRACT_2 && weights(neighborID) > acc) {
        acc += weights(neighborID)
      }
    }

    if (thresholdType == PruningUtils.ThresholdTypes.AVG) {
      acc /= neighborsNum
    } else if (thresholdType == PruningUtils.ThresholdTypes.MAX_FRACT_2) {
      acc /= 2.0
    }
    acc
  }

  /**
    * block pruning
    * @param profileBlocksFiltered    profile blocks
    * @param blockProfilesMap         block index as (blockID, profileIDs)
    * @param maxID                    maximum profile ID
    * @param groundTruth              ground truth
    * @param thresholdType            threshold type
    * @param weightType               weight type
    * @param profileBlocksNumMap      profile and number of blocks map
    * @param useEntropy               if use entropy
    * @param blockEntropyMap          block entropy map
    * @param chi2divider              used only in the chiSquare weight method to compute the threshold
    * @param comparisonType           comparison type
    * @param thresholds               threshold for each profile
    * @return
    */
  def pruning(profileBlocksFiltered: RDD[ProfileBlocks],
              blockProfilesMap: Broadcast[scala.collection.Map[Long, Array[Set[Long]]]],
              maxID: Int,
              groundTruth: Broadcast[scala.collection.immutable.Set[(Long, Long)]],
              thresholdType: String,
              weightType: String,
              profileBlocksNumMap: Broadcast[scala.collection.Map[Long, Int]],
              useEntropy: Boolean,
              blockEntropyMap: Broadcast[scala.collection.Map[Long, Double]],
              chi2divider: Double,
              comparisonType: String,
              thresholds: Broadcast[scala.collection.Map[Long, Double]]
             ): RDD[(Double, Double, Iterable[UnweightedEdge])] = {
    profileBlocksFiltered.mapPartitions { partition =>
      val localWeights = Array.fill[Double](maxID + 1) { 0 }
      val neighbors = Array.ofDim[Int](maxID + 1)
      var neighborsNum = 0
      val entropies: Array[Double] = {
        if (useEntropy) {
          Array.fill[Double](maxID + 1) { 0.0 }
        } else {
          null
        }
      }
      partition.map { profileBlocks =>
          neighborsNum = CommonNodePruning.calcCBS(profileBlocks, blockProfilesMap, useEntropy, blockEntropyMap,
            localWeights, entropies, neighbors, firstStep = false)
          CommonNodePruning.calcWeights(profileBlocks, localWeights, neighbors, entropies, neighborsNum,
            blockProfilesMap, weightType, profileBlocksNumMap, useEntropy)
          val result = doPruning(profileBlocks.profileID, localWeights, neighbors, neighborsNum, groundTruth,
            weightType, comparisonType, thresholds, chi2divider)
          CommonNodePruning.doReset(localWeights, neighbors, entropies, useEntropy, neighborsNum)
          result
      }
    }
  }

  /**
    * perform pruning neighbors for each profile and return results
    * @param profileID        profile ID
    * @param weights          weights for each neighbor
    * @param neighbors        neighbors of the profile
    * @param neighborsNum     number of neighbors
    * @param groundTruth      ground truth
    * @param weightType       weight type
    * @param comparisonType   comparison type
    * @param thresholds       thresholds of profiles
    * @param chi2divider      used only in the chiSquare weight method to compute the threshold
    * @return                 retained number, real edges and retained edges
    */
  def doPruning(profileID: Long,
                weights: Array[Double],
                neighbors: Array[Int],
                neighborsNum: Int,
                groundTruth: Broadcast[scala.collection.immutable.Set[(Long, Long)]],
                weightType: String,
                comparisonType: String,
                thresholds: Broadcast[scala.collection.Map[Long, Double]],
                chi2divider: Double
               ): (Double, Double, Iterable[UnweightedEdge]) = {
    var count: Double = 0
    var gtCount: Double = 0
    var edges: List[UnweightedEdge] = Nil
    val profileThreshold = thresholds.value(profileID)
    if (weightType == PruningUtils.WeightTypes.chiSquare) {
      for (i <- 0 until neighborsNum) {
        val neighborID = neighbors(i)
        val neighborThreshold = thresholds.value(neighborID)
        val neighborWeight = weights(neighborID)
        val threshold = Math.sqrt(Math.pow(profileThreshold, 2) + Math.pow(neighborThreshold, 2)) / chi2divider
        if (neighborWeight >= threshold) {
          count += 1
          if (groundTruth != null && groundTruth.value.contains((profileID, neighborID))) {
            gtCount += 1
          }
          edges = UnweightedEdge(profileID, neighborID) :: edges
        }
      }
      (count, gtCount, edges)
    } else {
      for (i <- 0 until neighborsNum) {
        val neighborID = neighbors(i)
        val neighborThreshold = thresholds.value(neighborID)
        val neighborWeight = weights(neighborID)
        if ((comparisonType == PruningUtils.ComparisonTypes.AND &&
          neighborWeight >= neighborThreshold && neighborWeight >= profileThreshold)
          || (comparisonType == PruningUtils.ComparisonTypes.OR &&
          (neighborWeight >= neighborThreshold || neighborWeight >= profileThreshold))) {
          count += 1
          if (groundTruth != null && groundTruth.value.contains((profileID, neighborID))) {
            gtCount += 1
          }
          edges = UnweightedEdge(profileID, neighborID) :: edges
        }
      }
      (count, gtCount, edges)
    }

  }

}
