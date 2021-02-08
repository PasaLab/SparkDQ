package pasa.bigdata.dqlib.models.iforest

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Random

import org.apache.commons.math3.random.{RandomDataGenerator, RandomGeneratorFactory}
import org.apache.spark.SparkException
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

import pasa.bigdata.dqlib.models.CommonUtils.{DEFAULT_FEATURES_COL, DEFAULT_ANOMALY_SCORE_COL}
import pasa.bigdata.dqlib.models.CommonSummary


/*
 Isolation Forest is a ensemble-based fast anomaly detection method, with linear time complexity
 and high accuracy. This method is suitable for continuous numerical data.
  */

/**
  * Model of IF(isolation forest), including constructor, copy, write and get summary.
  * @param uid unique ID for the Model
  * @param _trees Param of trees for constructor
  */
class IForestModel(override val uid: String, private val _trees: Array[IFNode]) extends Model[IForestModel]
  with IForestParams {

  def trees: Array[IFNode] = _trees

  private var trainingSummary: Option[IForestSummary] = None

  def hasSummary: Boolean = trainingSummary.nonEmpty

  def summary: IForestSummary = trainingSummary.getOrElse {
    throw new SparkException(
      s"No training summary available for the ${this.getClass.getSimpleName}"
    )
  }

  private[iforest] def setSummary(summary: Option[IForestSummary]): this.type = {
    this.trainingSummary = summary
    this
  }

  /** add extra param to the Model */
  override def copy(extra: ParamMap): IForestModel = {
    val copied = copyValues(new IForestModel(uid, trees), extra)
    copied.setSummary(trainingSummary).setParent(parent)
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  /**
    * Predict if a particular sample is an outlier or not.
    * @param dataSet Input data which is a dataSet with n_samples rows. This dataSet must have a
    *                column named features, or call setFeaturesCol to set user defined feature
    *                column name. This column stores the feature values for each instance, users can
    *                use VectorAssembler to generate a feature column.
    * @return A new DataFrame with an anomaly score column
    */
  override def transform(dataSet: Dataset[_]): DataFrame = {
    val numSamples = $(numRows)
    val possibleMaxSamples = if ($(maxSamples) > 1.0) $(maxSamples) else $(maxSamples) * numSamples
    val broadModel = dataSet.sparkSession.sparkContext.broadcast(this)
    val scoreUDF = udf { features: Vector => {
      val normFactor = avgLength(possibleMaxSamples)
      val avgPathLength = broadModel.value.calAvgPathLength(features)
      Math.pow(2, - avgPathLength / normFactor) }
    }
    dataSet.withColumn($(anomalyScoreCol), scoreUDF(col($(featuresCol)))).drop($(featuresCol))
  }

  /**
    * Calculate an average path length for a given feature set in a forest.
    * @param features A Vector stores feature values.
    * @return Average path length.
    */
  private def calAvgPathLength(features: Vector): Double = {
    val avgPathLength = trees.map(
      ifNode => { calPathLength(features, ifNode, currentPathLength = 0) }
    ).sum / trees.length
    avgPathLength
  }

  /**
    * Calculate a path length for a given feature set in a tree.
    * @param features A Vector stores feature values.
    * @param ifNode Tree's root node.
    * @param currentPathLength Current path length.
    * @return Path length in this tree.
    */
  private def calPathLength(features: Vector, ifNode: IFNode, currentPathLength: Int): Double = ifNode match {
    case leafNode: IFLeafNode => currentPathLength + avgLength(leafNode.numInstance)
    case internalNode: IFInternalNode =>
      val attrIndex = internalNode.featureIndex
      if (features(attrIndex) < internalNode.featureValue) {
        calPathLength(features, internalNode.leftChild, currentPathLength + 1)
      } else {
        calPathLength(features, internalNode.rightChild, currentPathLength + 1)
      }
  }

  /**
    * A function to calculate an expected path length with a specific data size.
    * @param size Data size.
    * @return An expected path length.
    */
  private def avgLength(size: Double): Double = {
    if (size > 2) {
      val H = Math.log(size - 1) + IForestModel.EulerConstant
      2 * H - 2 * (size - 1) / size
    }
    else if (size == 2) 1.0
    else 0.0
  }

}


/**
  * object of isolation forest Model
  */
object IForestModel {

  val EulerConstant = 0.5772156649

}

/**
  * Isolation Forest (iForest) is a effective model that focuses on anomaly isolation.
  * iForest uses tree structure for modeling data, iTree isolates anomalies closer to
  * the root of the tree as compared to normal points.
  *
  * A anomaly score is calculated by iForest model to measure the abnormality of the
  * data instances. The higher, the more abnormal.
  *
  * More details about iForest can be found in paper
  * <a href="https://dl.acm.org/citation.cfm?id=1511387">Isolation Forest</a>
  *
  * iForest on Spark is trained via model-wise parallelism, and predicts a new Dataset via data-wise parallelism,
  * It is implemented in the following steps:
  * 1. Sampling data from a Dataset. Data instances are sampled and grouped for each iTree. As indicated in the paper,
  * the number samples for constructing each tree is usually not very large (default value 256). Thus we can construct
  * a sampled paired RDD, where each row key is tree index and row value is a group of sampled data instances for a tree.
  * 2. Training and constructing each iTree on parallel via a map operation and collect the iForest model in the driver.
  * 3. Predict a new Dataset on parallel via a map operation with the collected iForest model.
  *
  * @param uid unique ID for Model
  */
class IForest(override val uid: String) extends Estimator[IForestModel]
  with IForestParams {

  setDefault(
    numRows -> -1L,
    numTrees -> 100,
    maxSamples -> 1.0,
    maxFeatures -> 1.0,
    maxDepth -> 10,
    bootstrap -> false,
    seed -> 24,
    featuresCol -> DEFAULT_FEATURES_COL,
    anomalyScoreCol -> DEFAULT_ANOMALY_SCORE_COL
  )

  def setColumns(value: Array[String]): this.type = set(columns, value)

  def setNumRows(value: Long): this.type = set(numRows, value)

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setAnomalyScoreCol(value: String): this.type = set(anomalyScoreCol, value)

  def setNumTrees(value: Int): this.type = set(numTrees, value)

  def setMaxSamples(value: Double): this.type = set(maxSamples, value)

  def setMaxFeatures(value: Double): this.type = set(maxFeatures, value)

  def setMaxDepth(value: Int): this.type = set(maxDepth, value)

  def setBootstrap(value: Boolean): this.type = set(bootstrap, value)

  def setSeed(value: Long): this.type = set(seed, value)

  lazy val rng = new Random($(seed))

  var possibleMaxSamples = 0

  def this() = this(Identifiable.randomUID("IForest"))

  override def copy(extra: ParamMap): IForest = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  /**
    * Training a iforest model for a given dataset
    *
    * @param dataSet Input data which is a dataset with n_samples rows. This dataset must have a
    *                column named features, or call setFeaturesCol to set user defined feature
    *                column name. This column stores the feature values for each instance, users can
    *                use VectorAssembler to generate a feature column.
    * @return trained iforest model with an array of each tree's root node.
    */
  override def fit(dataSet: Dataset[_]): IForestModel = {
    var summary: IForestSummary = null
    try {
      if ($(numRows) < 0) {
        setNumRows(dataSet.count())
      }
      val assembler = new VectorAssembler().setInputCols($(columns)).setOutputCol($(featuresCol))
      val featuredDataSet = assembler.transform(dataSet)

      val rddPerTree: RDD[(Int, Array[Vector])] = splitData(featuredDataSet)
      /*
        Each iTree of the iForest will be built on parallel and collected in the driver, so we have to consider storage
        cost on driver.
        Approximate memory usage for iForest model is calculated, a warning will be raised if iForest is too large.
        In the worst situation, the number of nodes is about 2 times the number of sample instances. Each Node is 32Bytes.
      */
      //    val usageMemory = $(numTrees) * 2 * possibleMaxSamples * 32 / (1024 * 1024)
      //    if (usageMemory > 2048) {
      //      println("The isolation forest stored on the driver will exceed 2GB memory. " +
      //        "If your machine can not bear memory consuming, please try small numTrees or maxSamples.")
      //    }

      val _trees = rddPerTree.map {
        case (treeId: Int, points: Array[Vector]) =>
          val random = new Random(rng.nextInt + treeId)
          val (trainData, featureIdxArr) = sampleFeatures(points, $(maxFeatures), random)
          val longestPath = Math.ceil(Math.log(Math.max(2, points.length)) / Math.log(2)).toInt
          val possibleMaxDepth = if ($(maxDepth) > longestPath) longestPath else $(maxDepth)
          //        if(possibleMaxDepth != $(maxDepth)) {
          //          println("building iTrees using possible max depth " + possibleMaxDepth + ", instead of " + $(maxDepth))
          //        }
          val numFeatures = trainData.head.length
          val constantFeatures = Array.range(0, numFeatures + 1)
          constantFeatures(numFeatures) = 0
          iTree(trainData, 0, possibleMaxDepth, constantFeatures, featureIdxArr, random)
      }.collect()
      val model = copyValues(new IForestModel(uid, _trees).setParent(this))
      val dataWithScores = model.transform(featuredDataSet)
      summary = new IForestSummary(dataWithScores)
      model.setSummary(Some(summary))
      model
    } catch {
      case e: Exception =>
        val model = copyValues(new IForestModel(uid, Array.empty[IFNode]))
        summary = new IForestSummary(dataSet.toDF())
        summary.setSuccess(false)
        summary.setMessage(e.getMessage)
        model.setSummary(Some(summary))
        model
    }
  }

  /**
    * Sample and split data to $numTrees groups, each group will build a tree.
    *
    * @param dataSet Training DataSet
    * @return A paired RDD, where key is the tree index, value is an array of data instances(represented by feature)
    *         for training a iTree.
    */
  private[iforest] def splitData(dataSet: Dataset[_]): RDD[(Int, Array[Vector])] = {

    require($(maxSamples) <= $(numRows), s"The max samples must be less then total number of the input data")

    val fraction =
      if ($(maxSamples) > 1) $(maxSamples) / $(numRows)
      else $(maxSamples)

    possibleMaxSamples = (fraction * $(numRows)).toInt

    // random generator
    val advancedRgn = new RandomDataGenerator(
      RandomGeneratorFactory.createRandomGenerator(new java.util.Random(rng.nextLong()))
    )

    val rddPerTree = {
      /*
        SampledIndices is a two-dimensional array, that generates sampling row indices for each iTree.

        E.g. [[1, 3, 6, 4], [6, 4, 2, 5]] indicates that the first tree has data consists of the 1, 3, 6, 4 row
        samples, the second tree has data consists of the 6, 4, 3, 5 row samples.

        if bootstrap is true, sample with replacement, else without replacement.
        Note: sampleIndices will occupy about maxSamples * numTrees * 8 byte memory in the driver.
      */
      val sampleIndices = if ($(bootstrap)) {
        Array.tabulate($(numTrees)) { _ =>
          Array.fill(possibleMaxSamples) {
            advancedRgn.nextLong(0, $(numRows))
          }
        }
      } else {
        Array.tabulate($(numTrees)) { _ =>
          reservoirSampleAndCount(Range.Long(0, $(numRows), 1).iterator,
            possibleMaxSamples, rng.nextLong)
        }
      }

      /*
        rowInfo structure is a Map in which key is rowId identifying each row,
        and value is a SparseVector indicating which trees will sample this row for how many times.
        SparseVector is a sparse vector constructed by (numTrees, treeIdArray, numCopiesArray), where
        - treeIdArray indicates that which trees will sample this row;
        - numCopiesArray indicates how many times corresponding tree will sample this row.

        E.g., Map{1 -> SparseVector(100, [1, 3, 5], [3, 6, 1]])} means that there are 100
        trees to construct a forest, target row is 1, tree 1 will sample row 1 three times,
        tree 3 will sample row 1 six times, tree 5 will sample row 1 one time.
       */
      val rowInfo = sampleIndices.zipWithIndex.flatMap {
        case (indices: Array[Long], treeId: Int) => indices.map(rowIndex => (rowIndex, treeId))
      }.groupBy(_._1).mapValues(pairs => pairs.map(_._2)).map {
        case (rowIndex: Long, treeIds: Array[Int]) =>
          val treeIdWithNumCopies = treeIds.map(treeId => (treeId, 1.0))
            .groupBy(_._1).map { case (treeId: Int, tmp: Array[(Int, Double)]) =>
            tmp.reduce((x, y) => (treeId, x._2 + y._2))
          }.toSeq
          (rowIndex, Vectors.sparse($(numTrees), treeIdWithNumCopies))
      }

      val broadRowInfo = dataSet.sparkSession.sparkContext.broadcast(rowInfo)
      dataSet.select(col($(featuresCol))).rdd.map { case Row(point: Vector) => point }.zipWithIndex()
        .filter { case (_: Vector, rowIndex: Long) => broadRowInfo.value.contains(rowIndex) }
        .flatMap { case (point: Vector, rowIndex: Long) =>
          val rowInEachTree = broadRowInfo.value(rowIndex).asInstanceOf[SparseVector]
          rowInEachTree.indices.zip(rowInEachTree.values).map {
            case (treeId: Int, numCopies: Double) => (treeId, Array.fill(numCopies.toInt)(point))
          }
        }.reduceByKey((arr1, arr2) => arr1 ++ arr2)
    }
    rddPerTree
  }

  /**
    * Sample features to train a tree. Return sampled feature result and featureIdx.
    */
  private[iforest] def sampleFeatures(data: Array[Vector], maxFeatures: Double,
                                      random: Random = new Random()): (Array[Array[Double]], Array[Int]) = {
    val numFeatures = data.head.size
    val subFeatures: Int = {
      if (maxFeatures <= 1) (maxFeatures * numFeatures).toInt
      else if (maxFeatures > numFeatures) {
        println("maxFeatures is larger than the numFeatures, using all features instead")
        numFeatures
      }
      else maxFeatures.toInt
    }
    if (subFeatures == numFeatures) {
      (data.map(vector => vector.asInstanceOf[DenseVector].values), Array.range(0, numFeatures))
    } else {
      val featureIdx = random.shuffle((0 until numFeatures).toList).take(subFeatures)
      val sampledFeatures = mutable.ArrayBuilder.make[Array[Double]]
      data.foreach(vector => {
        val sampledValues = new Array[Double](subFeatures)
        featureIdx.zipWithIndex.foreach(feature => sampledValues(feature._2) = vector(feature._1))
        sampledFeatures += sampledValues
      })
      (sampledFeatures.result(), featureIdx.toArray)
    }
  }

  /**
    * Builds a tree
    *
    * @param data               Input data, a two dimensional array, can be regarded as a table, each row
    *                           is an instance, each column is a feature value.
    * @param currentPathLength  current node's path length
    * @param maxDepth height    limit during building a tree
    * @param constantFeatures   an array stores constant features indices, last element N is number of constant
    *                           features, first N positions store constant features indices.
    * @param featureIdxArr      an array stores the mapping from the sampled feature idx to the origin feature idx
    * @param random             random for generating iTree
    * @return                   tree's root node
    */
  private[iforest] def iTree(data: Array[Array[Double]], currentPathLength: Int, maxDepth: Int,
                             constantFeatures: Array[Int], featureIdxArr: Array[Int], random: Random): IFNode = {
    var constantFeatureIndex = constantFeatures.last
    /*
      The condition of leaf node
      1. current path length exceeds max depth
      2. the number of data can not be split again
      3. there are no non-constant features to draw
    */
    if (currentPathLength >= maxDepth || data.length <= 1) {
      new IFLeafNode(data.length)
    } else {
      val numFeatures = data.head.length
      var attrMin = 0.0
      var attrMax = 0.0
      var attrIndex = -1
      var findConstant = true
      while (findConstant && numFeatures != constantFeatureIndex) {
        // select randomly a feature index from constantFeatureIndex to numFeatures - 1, skip constant features indices
        val idx = random.nextInt(numFeatures - constantFeatureIndex) + constantFeatureIndex
        attrIndex = constantFeatures(idx)
        val features = Array.tabulate(data.length)(i => data(i)(attrIndex))
        attrMin = features.min
        attrMax = features.max
        if (attrMin == attrMax) {
          constantFeatures(idx) = constantFeatures(constantFeatureIndex)
          constantFeatures(constantFeatureIndex) = attrIndex
          constantFeatureIndex += 1
          constantFeatures(constantFeatures.length - 1) = constantFeatureIndex
        } else {
          findConstant = false
        }
      }
      if (numFeatures == constantFeatureIndex) new IFLeafNode(data.length)
      else {
        // select randomly a feature value between (attrMin, attrMax)
        val attrValue = random.nextDouble() * (attrMax - attrMin) + attrMin
        // split data according to the attrValue
        val leftData = data.filter(point => point(attrIndex) < attrValue)
        val rightData = data.filter(point => point(attrIndex) >= attrValue)
        // recursively build a tree
        new IFInternalNode(
          iTree(leftData, currentPathLength + 1, maxDepth, constantFeatures.clone(), featureIdxArr, random),
          iTree(rightData, currentPathLength + 1, maxDepth, constantFeatures.clone(), featureIdxArr, random),
          featureIdxArr(attrIndex), attrValue)
      }
    }
  }

}

trait IForestParams extends Params {

  /**
    * Data related parameters
    */

  final val columns: StringArrayParam = new StringArrayParam(parent = this, name = "columns",
    doc = "columns of all features", ParamValidators.arrayLengthGt(0))

  def getColumns: Array[String] = $(columns)

  final val numRows: LongParam = new LongParam(parent = this, name = "numRows", doc = "number of total rows")

  def getNumRows: Long = $(numRows)

  final val featuresCol: Param[String] = new Param[String](parent = this, name = "featuresCol", doc =
    "column of features vector")

  def getFeaturesCol: String = $(featuresCol)

  final val anomalyScoreCol: Param[String] = new Param[String](parent = this, name = "anomalyScoreCol", doc =
    "column of anomaly score")

  def getAnomalyScoreCol: String = $(anomalyScoreCol)

  final val numTrees: IntParam =
    new IntParam(parent = this, name = "numTrees", doc = "number of trees", ParamValidators.gt(lowerBound = 0))

  def getNumTrees: Int = $(numTrees)

  /**
    * The number of samples to draw from data to train each tree (>0).
    *
    * If <= 1, the algorithm will draw maxSamples * totalSample samples.
    *
    * If > 1, the algorithm will draw maxSamples samples.
    *
    * This parameter will affect the driver's memory when splitting data.
    *
    * The total memory is about maxSamples * numTrees * 4 + maxSamples * 8 bytes.
    *
    * @group param
    */
  final val maxSamples: DoubleParam =
    new DoubleParam(parent = this, name = "maxSamples", doc = "the number of samples to " +
      "draw from data to train each tree. Must be > 0. If <= 1, " +
      "then draw maxSamples * totalSamples. If > 1, then draw " +
      "maxSamples samples.", ParamValidators.gt(lowerBound = 0.0))

  def getMaxSamples: Double = $(maxSamples)

  /**
    * The number of features to draw from data to train each tree (>0).
    *
    * If <= 1, the algorithm will draw maxFeatures * totalFeatures features.
    *
    * If > 1, the algorithm will draw maxFeatures features.
    * @group param
    */
  final val maxFeatures: DoubleParam =
    new DoubleParam(parent = this, name = "maxFeatures", doc = "the number of features to" +
      " draw from data to train each tree. Must be > 0. If <= 1, " +
      "then draw maxFeatures * totalFeatures. If > 1, then draw " +
      "maxFeatures features.", ParamValidators.gt(lowerBound = 0.0))

  def getMaxFeatures: Double = $(maxFeatures)

  /**
    * The height limit used in constructing a tree (>0).
    *
    * The default value will be about log2(numSamples).
    * @group param
    */
  final val maxDepth: IntParam =
    new IntParam(parent = this, name = "maxDepth", doc = "the height limit used in constructing" +
      " a tree. Must be > 0", ParamValidators.gt(lowerBound = 0))

  def getMaxDepth: Int = $(maxDepth)

  /**
    * If true, individual trees are fit on random subsets of the training data
    * sampled with replacement. If false, sampling without replacement is performed.
    * @group param
    */
  final val bootstrap: BooleanParam =
    new BooleanParam(parent = this, name = "bootstrap", doc = "If false, samples in a tree " +
      "are not the same, i.e. draw without replacement. If true, samples in a tree" +
      " are drawn with replacement.")

  def getBootstrap: Boolean = $(bootstrap)

  /**
    * The seed used by the random number generator.
    * @group param
    */
  final val seed: LongParam = new LongParam(parent = this, name = "seed", doc = "random seed")

  def getSeed: Long = $(seed)

  /**
    * Reservoir sampling implementation that also returns the input size.
    * @param input input size, range of 0 ~ numSamples-1
    * @param k reservoir size
    * @param seed random seed
    * @return (samples, input size)
    */
  def reservoirSampleAndCount[T: ClassTag](input: Iterator[T], k: Int,
                                           seed: Long = Random.nextLong()): Array[T] = {
    val reservoir = new Array[T](k)
    // First put the first k elements in the reservoir.
    var i = 0
    while (i < k && input.hasNext) {
      val item = input.next()
      reservoir(i) = item
      i += 1
    }

    // If we have consumed all the elements, return them. Otherwise do the replacement randomly.
    if (i < k) {
      // If input size < k, trim the array to return an array of input size
      val trimReservoir = new Array[T](i)
      System.arraycopy(reservoir, 0, trimReservoir, 0, i)
      trimReservoir
    } else {
      // If input size > k, continue the sampling process and replace randomly.
      var l = i.toLong
      val rand = new Random(seed)
      while (input.hasNext) {
        val item = input.next()
        l += 1
        // There are k elements in the reservoir, and the l-th element has been consumed.
        // It should be chosen with probability k/l.
        // The expression below is a random long chosen uniformly from [0,l).
        // Finally each element's chosen probability is equal.
        val replacementIndex = (rand.nextDouble() * l).toLong
        if (replacementIndex < k) {
          reservoir(replacementIndex.toInt) = item
        }
      }
      reservoir
    }
  }
}

class IForestSummary(@transient val dataWithScores: DataFrame) extends CommonSummary
  with Serializable {

}
