package pasa.bigdata.dqlib.models.entities

import scala.collection.JavaConverters._

import info.debatty.java.stringsimilarity._
import org.apache.spark.graphx.Graph
import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StructField, StructType, StringType}

import pasa.bigdata.dqlib.models.CommonUtils._
import pasa.bigdata.dqlib.models.CommonSummary
import pasa.bigdata.dqlib.models.entities.building.{LSH, TokenBlocking}
import pasa.bigdata.dqlib.models.entities.refinement.Pruning.{PruningUtils, WNP}
import pasa.bigdata.dqlib.models.entities.refinement.{BlockFiltering, BlockPurging}
import pasa.bigdata.dqlib.models.entities.structures.{KeyValue, Profile}


class EntityResolutionModel(override val uid: String) extends Model[EntityResolutionModel] with EntityResolutionParams
  with CommonSummary {

  private[entities] var numOfEntities: Long = _

  private var trainingSummary: Option[EntityResolutionSummary] = None

  private[entities] def setSummary(summary: Option[EntityResolutionSummary]): this.type = {
    this.trainingSummary = summary
    this
  }

  def hasSummary: Boolean = trainingSummary.nonEmpty

  def summary: EntityResolutionSummary = trainingSummary.getOrElse(
    throw new SparkException(s"No training summary available for the ${this.getClass.getSimpleName}"))

  override def copy(extra: ParamMap): EntityResolutionModel = {
    val copied = copyValues(new EntityResolutionModel(uid), extra)
    copied.setSummary(trainingSummary).setParent(parent)
  }

  override def transformSchema(schema: StructType): StructType = schema

  override def transform(dataSet: Dataset[_]): DataFrame = {
    try {
      if (!$(taskType).equals(TaskTypes.DETECT) && !$(taskType).equals(TaskTypes.REPAIR)) {
        success = false
        message = "Invalid task type %s" format $(taskType)
        return dataSet.toDF()
      }
      val profiles = loadProfiles(dataSet.toDF(), $(columns), $(indexCol))

      // token blocking
      val blocks = TokenBlocking.createBlocks(profiles)
      val useEntropy = false

      // loose meta blocking
      val maxID = profiles.map(_.profileID).max()
//      val useEntropy = true
//      val clusters = LSH.clusterSimilarAttributes(
//        profiles = profiles,
//        numHashes = $(numHashes),
//        targetThreshold = $(targetThreshold),
//        maxFactor = $(maxFactor),
//        numBands = $(numBands),
//        computeEntropy = useEntropy
//      )

      // create blocks according to attributes cluster, token key is token value + clusterID
//      val blocks = TokenBlocking.createBlocksCluster(profiles, clusters)

      // purging over-sized blocks by Comparison Cardinality
      val blocksPurged = BlockPurging.blockPurging(blocks, $(smoothFactor))

      // filtering 20% largest blocks for each profile
      val profileBlocks = Converters.blocksToProfileBlocks(blocksPurged)
      val profileBlocksFiltered = BlockFiltering.blockFiltering(profileBlocks, $(keepRate))
      // turn back to blocks
      val blocksAfterFiltering = Converters.profilesBlockToBlocks(profileBlocksFiltered)

      // meta-blocking
      val sc = blocksAfterFiltering.sparkContext
      val blockProfilesMap = sc.broadcast(blocksAfterFiltering.map(block => (block.blockID, block.profiles)).collectAsMap())
      val profileBlocksNumMap = sc.broadcast(profileBlocksFiltered.map(profileBlocks =>
        (profileBlocks.profileID, profileBlocks.blocks.size)).collectAsMap())
      val blockEntropyMap = {
        if (useEntropy) {
          sc.broadcast(blocksAfterFiltering.map(block => (block.blockID, block.entropy)).collectAsMap())
        } else {
          null
        }
      }

      val edgesAndCount = WNP.WNP(profileBlocksFiltered, blockProfilesMap, maxID.toInt, null,
        $(thresholdType), $(weightType), profileBlocksNumMap, useEntropy, blockEntropyMap, $(chi2divider),
        $(comparisonType))
      val candidatePairs = edgesAndCount.flatMap(_._3).map(x => (x.firstProfileID, x.secondProfileID))
      /*
      // for test
      val numCandidates = edgesAndCount.map(_._1).sum()
      val perfectMatches = edgesAndCount.map(_._2).sum()
      val pc: Float = perfectMatches.toFloat / newGTSize.toFloat
      val pq: Float = perfectMatches.toFloat / numCandidates.toFloat
      */

      // matcher
      val profilesWithID = profiles.map(x => (x.profileID, x))
      if ($(taskType).equals(TaskTypes.REPAIR)) {
        profilesWithID.persist()
      }
      // leftProfileID, rightProfileID, leftProfile, rightProfile
      val pairsWithProfile = candidatePairs.leftOuterJoin(profilesWithID)
        .map(x => (x._2._1, (x._1, x._2._2))).leftOuterJoin(profilesWithID)
        .map(x => (x._2._1._1, x._1, x._2._1._2, x._2._2))

      val pairsWithSimilarity = pairsWithProfile.map(pair => {
        val leftProfile = pair._3.get
        val rightProfile = pair._4.get
        val similarity = EntityResolution.calcSimilarity(leftProfile, rightProfile, $(similarityType), $(columns))
        (pair._1, pair._2, leftProfile, rightProfile, similarity)
      })
      val threshold = $(similarityThreshold)
      val matchingPairs = pairsWithSimilarity.filter(_._5 >= threshold).map(x => (x._1, x._2))

      // cluster
      // all edges means undirected graph
      val edges = matchingPairs.union(matchingPairs.map(_.swap))
      val graph = Graph.fromEdgeTuples(edges, defaultValue = 1)
      // clusterID is the smallest vertexID in the cluster
      val vertexCluster = graph.connectedComponents().vertices
      numOfEntities = vertexCluster.map(x => x._2).distinct().count()
      if ($(taskType).equals(TaskTypes.DETECT)) {
        dataSet.toDF()
      } else {
        // extract entities
        val profileAndRealID = profilesWithID.map(x => (x._1, x._2.realID))
        // realID and clusterID
        val realAndClusterID = vertexCluster.leftOuterJoin(profileAndRealID).map(x => Row(x._2._2.get, x._2._1))
        val targetIndexType = dataSet.schema($(indexCol)).dataType
        // real ID and cluster ID
        val clusterDF = buildClusterDF(realAndClusterID, $(indexCol), $(clusterCol), dataSet.sparkSession)
          .withColumn($(indexCol), col($(indexCol)).cast(targetIndexType)).alias("df_cluster")

        // target columns include indexCol, columns for extracting and clusterCol
        // here return clustering result directly,
        // how to deduplicate is up to user,
        // maybe merge or calculate mode, call the aggregation functions provided later
        val targetDF = clusterDF.join(dataSet.select($(indexCol), $(columns): _*),
          clusterDF($(indexCol)) === dataSet($(indexCol)), "left_outer")
          .select("df_cluster." + $(indexCol), $(columns)++Array($(clusterCol)): _*)

        targetDF
//        val aggregate_functions = $(columns).map(x => StatefulMode(col(x)).alias(x))
//        targetDF.groupBy($(clusterCol))
//          .agg(aggregate_functions.head, aggregate_functions.tail: _*)
//          .drop($(clusterCol))
      }
    } catch {
      case e: Exception =>
        success = false
        message = e.getMessage
        dataSet.toDF
    }

  }

  def buildClusterDF(rdd: RDD[Row], indexCol: String, clusterCol: String, sparkSession: SparkSession): DataFrame = {
    val resultSchema = StructType(List(
      StructField(indexCol, StringType, nullable = false),
      StructField(clusterCol, LongType, nullable = false)
    ))
    // val spark = SparkSession.builder().getOrCreate()
    val x = sparkSession.createDataFrame(rdd, resultSchema)
    x
  }

  /**
    * Load profiles from DataFrame
    * @param dataSet              DataFrame
    * @param columns              target columns
    * @param indexCol             index column
    * @param explodeInnerFields   if or not to split inner fields
    * @param innerSeparator       inner separator for splitting
    * @return
    */
  def loadProfiles(dataSet: DataFrame, columns: Array[String], indexCol: String, explodeInnerFields: Boolean = false,
                   innerSeparator: String = DEFAULT_SEPARATOR): RDD[Profile] = {
    dataSet.select(indexCol, columns: _*).rdd.zipWithIndex.map { case (row, pid) =>
      val attributes: scala.collection.mutable.MutableList[KeyValue] = rowToAttributes(row, columns, explodeInnerFields,
        innerSeparator)
      val realId = row.get(0).toString
      Profile(pid, attributes, realId)
    }
  }

  /**
    * Transform row data to attributes
    * @param row                  row data
    * @param columns              target columns
    * @param explodeInnerFields   if split string attribute value
    * @param innerSeparator       separator if split string attribute value
    * @return
    */
  def rowToAttributes(row: Row, columns: Array[String], explodeInnerFields: Boolean = false,
                      innerSeparator: String = DEFAULT_SEPARATOR): scala.collection.mutable.MutableList[KeyValue] = {
    val attributes: scala.collection.mutable.MutableList[KeyValue] = new scala.collection.mutable.MutableList()
    for (i <- 1 until row.size) {
        val value = row.get(i)
        val attributeKey = columns(i-1)
        if (value != null) {
          value match {
            case listOfAttributes: Iterable[Any] =>
              listOfAttributes map { attributeValue =>
                  attributes += KeyValue(attributeKey, attributeValue.toString)
              }
            case stringAttribute: String =>
              if (explodeInnerFields) {
                stringAttribute.split(innerSeparator) map { attributeValue =>
                    attributes += KeyValue(attributeKey, attributeValue)
                }
              } else {
                attributes += KeyValue(attributeKey, stringAttribute)
              }
            case singleAttribute =>
              attributes += KeyValue(attributeKey, singleAttribute.toString)
          }
        }
      }
    attributes
  }

}

class EntityResolution(override val uid: String) extends Estimator[EntityResolutionModel] with EntityResolutionParams {

  def this() = this(Identifiable.randomUID("EntityResolution"))

  setDefault(
    indexCol -> DEFAULT_INDEX_COL,
    similarityType -> SimilarityTypes.COSINE,
    similarityThreshold -> 0.5,
    taskType -> TaskTypes.DETECT,
    clusterCol -> DEFAULT_CLUSTER_COL,
    numHashes -> 128,
    targetThreshold -> 0.3,
    maxFactor -> 1.0,
    numBands -> -1,
    smoothFactor -> 1.015,
    keepRate -> 0.8,
    thresholdType -> PruningUtils.ThresholdTypes.AVG,
    weightType -> PruningUtils.WeightTypes.CBS,
    comparisonType -> PruningUtils.ComparisonTypes.OR,
    chi2divider -> 2.0
  )

  def setIndexCol(value: String): this.type = { set(indexCol, value) }

  def setColumns(value: Array[String]): this.type = { set(columns, value) }

  def setTaskType(value: String): this.type = { set(taskType, value) }

  def setNumHashes(value: Int): this.type = { set(numHashes, value) }

  def setTargetThreshold(value: Double): this.type = { set(targetThreshold, value) }

  def setMaxFactor(value: Double): this.type = { set(maxFactor, value) }

  def setNumBands(value: Int): this.type = { set(numBands, value) }

  def setSmoothFactor(value: Double): this.type = { set(smoothFactor, value) }

  def setKeepRate(value: Double): this.type = { set(keepRate, value) }

  def setThresholdType(value: String): this.type = { set(thresholdType, value) }

  def setWeightType(value: String): this.type = { set(weightType, value) }

  def setComparisonType(value: String): this.type = { set(comparisonType, value) }

  def setChi2Divider(value: Double): this.type = { set(chi2divider, value) }

  def setSimilarityType(value: String): this.type = { set(similarityType, value) }

  def setSimilarityThreshold(value: Double): this.type = { set(similarityThreshold, value) }

  def setClusterCol(value: String): this.type = { set(clusterCol, value) }

  override def copy(extra: ParamMap): Estimator[EntityResolutionModel] = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = schema

  def fit(dataSet: Dataset[_]): EntityResolutionModel = {
    val model = copyValues(new EntityResolutionModel(uid).setParent(this))
    val summary = new EntityResolutionSummary(model.transform(dataSet))
    summary.setNumOfEntities(model.numOfEntities)
    summary.setSuccess(model.success)
    summary.setMessage(model.message)
    model.setSummary(Some(summary))
    model
  }

}

object EntityResolution {

  /**
    * Calculate distance between two profiles
    * @param profile1         profile1
    * @param profile2         profile2
    * @param similarityType   similarity type
    * @param attributes       attributes
    * @return
    */
  def calcSimilarity(profile1: Profile, profile2: Profile, similarityType: String, attributes: Array[String])
  : Double = {
    var similarity: Double = 0.0
    if (similarityType == SimilarityTypes.COSINE) {
      val attributesCountMap1 = profile1.toAttributesCountMap
      val attributesCountMap2 = profile2.toAttributesCountMap
      val cosine = new Cosine()
      for (attr <- attributes) {
        if (attributesCountMap1.contains(attr) && attributesCountMap2.contains(attr)) {
          val map1 = attributesCountMap1(attr).map { case (k, v) => k -> int2Integer(v) }.asJava
          val map2 = attributesCountMap2(attr).map { case (k, v) => k -> int2Integer(v) }.asJava
          similarity += cosine.similarity(map1, map2)
        }
      }
    } else if (similarityType == SimilarityTypes.JACCARD_INDEX) {
      val jaccard = new Jaccard()
      val attributesMap1 = profile1.toAttributesMap
      val attributesMap2 = profile2.toAttributesMap
      for (attr <- attributes) {
        if (attributesMap1.contains(attr) && attributesMap2.contains(attr)) {
          val value1 = attributesMap1(attr)
          val value2 = attributesMap2(attr)
          similarity += jaccard.similarity(value1, value2)
        }
      }
    } else if (similarityType == SimilarityTypes.JARO_WINKLER) {
      val jaroWinkler = new JaroWinkler()
      val attributesMap1 = profile1.toAttributesMap
      val attributesMap2 = profile2.toAttributesMap
      for (attr <- attributes) {
        if (attributesMap1.contains(attr) && attributesMap2.contains(attr)) {
          val value1 = attributesMap1(attr)
          val value2 = attributesMap2(attr)
          similarity += jaroWinkler.similarity(value1, value2)
        }
      }
    } else if (similarityType == SimilarityTypes.NORMALIZED_LEVENSHTEIN) {
      val normLeven = new NormalizedLevenshtein()
      val attributesMap1 = profile1.toAttributesMap
      val attributesMap2 = profile2.toAttributesMap
      for (attr <- attributes) {
        if (attributesMap1.contains(attr) && attributesMap2.contains(attr)) {
          val value1 = attributesMap1(attr)
          val value2 = attributesMap2(attr)
          similarity += normLeven.similarity(value1, value2)
        }
      }
    } else if (similarityType == SimilarityTypes.SORENSEN_DICE_COEFFICIENT) {
      val sorensenDice = new SorensenDice()
      val attributesMap1 = profile1.toAttributesMap
      val attributesMap2 = profile2.toAttributesMap
      for (attr <- attributes) {
        if (attributesMap1.contains(attr) && attributesMap2.contains(attr)) {
          val value1 = attributesMap1(attr)
          val value2 = attributesMap2(attr)
          similarity += sorensenDice.similarity(value1, value2)
        }
      }
    } else {
      throw new Exception("Unsupported similarity type %s".format(similarityType))
    }

    similarity / attributes.length
  }

}

trait EntityResolutionParams extends Params {

  final val columns: StringArrayParam = new StringArrayParam(parent = this, name = "columns",
    doc = "all related columns", ParamValidators.arrayLengthGt(0))

  def getColumns: Array[String] = $(columns)

  final val indexCol: Param[String] = new Param[String](parent = this, name = "indexCol", doc = "index column")

  def getIndexCol: String = $(indexCol)

  final val taskType: Param[String] = new Param[String](parent = this, name = "taskType", doc = "task type")

  def getTaskType: String = $(taskType)

  /**
    * Loose meta blocking params
    */
  final val numHashes: Param[Int] = new Param[Int](parent = this, name = "numHashes", doc = "number of hashes")

  def getNumHashes: Int = $(numHashes)

  final val targetThreshold: Param[Double] = new Param[Double](parent = this, name = "targetThreshold",
    doc = "target threshold")

  def getTargetThreshold: Double = $(targetThreshold)

  final val maxFactor: Param[Double] = new Param[Double](parent = this, name = "maxFactor", doc = "maximum factor")

  def getMaxFactor: Double = $(maxFactor)

  final val numBands: Param[Int] = new Param[Int](parent = this, name = "numBands", doc = "number of bands")

  def getNumBands: Int = $(numBands)

  /**
    * purging and filtering params
    */
  final val smoothFactor: Param[Double] = new Param[Double](parent = this, name = "smoothFactor", doc = "smooth factor")

  def getSmoothFactor: Double = $(smoothFactor)

  final val keepRate: Param[Double] = new Param[Double](parent = this, name = "keepRate", doc = "keep rate")

  def getKeepRate: Double = $(keepRate)

  /**
    * meta-blocking params
    */
  final val thresholdType: Param[String] = new Param[String](parent = this, name = "thresholdType",
    doc = "threshold type")

  def getThresholdType: String = $(thresholdType)

  final val chi2divider: Param[Double] = new Param[Double](parent = this, name = "chi2divider", doc = "chi2divider")

  def getChi2Divider: Double = $(chi2divider)

  final val weightType: Param[String] = new Param[String](parent = this, name = "weightType", doc = "weight type")

  def getWeightType: String = $(weightType)

  final val comparisonType: Param[String] = new Param[String](parent = this, name = "comparisonType",
    doc = "comparison type")

  def getComparisonType: String = $(comparisonType)

  /**
    * matching params
    */
  final val similarityType: Param[String] = new Param[String](parent = this, name = "similarityType", doc= "similarity "
    + "type")

  def getSimilarityType: String = $(similarityType)

  final val similarityThreshold: Param[Double] = new Param[Double](parent = this, name = "similarityThreshold",
    doc = "similarity threshold")

  def getSimilarityThreshold: Double = $(similarityThreshold)

  final val clusterCol: Param[String] = new Param[String](parent = this, name = "clusterCol", doc = "cluster column")

  def getClusterCol: String = $(clusterCol)

}

class EntityResolutionSummary(@transient val targetData: DataFrame) extends CommonSummary with Serializable {

  var numOfEntities: Long = _

  def setNumOfEntities(value: Long): Unit = {
    numOfEntities = value
  }

}

