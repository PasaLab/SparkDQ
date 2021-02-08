package pasa.bigdata.dqlib.models.entities.refinement

import org.apache.spark.rdd.RDD
import pasa.bigdata.dqlib.models.entities.structures.BlockAbstract

object BlockPurging {

  /**
    * Remove over-sized blocks
    * @param blocks         blocks
    * @param smoothFactor   smooth factor
    * @return
    */
  def blockPurging(blocks: RDD[BlockAbstract], smoothFactor: Double): RDD[BlockAbstract] = {
    val blockComparisonsAndSizes = blocks.map(block => (block.getComparisonSize, block.size))
    // take comparisons as comparison level
    val blockComparisonsAndSizesPerComparisonLevel = blockComparisonsAndSizes.map {
      blockComparisonAndSize => (blockComparisonAndSize._1, blockComparisonAndSize)
    }
    // sum comparisons and sizes for each comparison level, sort by ascending order
    val totalBlockComparisonsAndSizesPerComparisonLevel = blockComparisonsAndSizesPerComparisonLevel
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).sortBy(_._1).collect().toList
    val totalBlockComparisonsAndSizesPerComparisonLevelAdded =
      sumPrecedentLevels(totalBlockComparisonsAndSizesPerComparisonLevel).toArray
    val purgingThreshold = calcMaxNumOfComparisons(totalBlockComparisonsAndSizesPerComparisonLevelAdded, smoothFactor)
    blocks.filter(block => block.getComparisonSize <= purgingThreshold)
  }

  /**
    * Sum precedent levels' comparisons and sizes
    * @param input      total comparisons and sizes for each comparison level
    * @return
    */
  def sumPrecedentLevels(input: Iterable[(Double, (Double, Double))]): Iterable[(Double, (Double, Double))] = {
    if (input.isEmpty) {
      input
    } else {
      input.tail.scanLeft(input.head)((acc, x) => (x._1, (x._2._1 + acc._2._1, x._2._2 + acc._2._2)))
    }
  }

  /**
    * Compute block threshold for ignoring
    * @param input            sum precedent levels' comparisons and sizes (comparisonLevel, (sumComparisons, sumSizes))
    * @param smoothFactor     for tuning
    * @return
    */
  def calcMaxNumOfComparisons(input: Array[(Double, (Double, Double))], smoothFactor: Double): Double = {
    var curLevel: Double = 0
    var curCC: Double = 0   // comparison cardinality
    var curBC: Double = 0   // block cardinality

    var preLevel: Double = 0
    var preCC: Double = 0
    var preBC: Double = 0
    val inputSize = input.length

    for (i <- inputSize-1 to 0 by -1) {
      preLevel = curLevel
      preCC = curCC
      preBC = curBC

      curLevel = input(i)._1
      curCC = input(i)._2._1
      curBC = input(i)._2._2
      // until CC/BC is stable
      if (curBC * preCC < smoothFactor * preBC * curCC)
        return preLevel
    }
    preLevel
  }

}
