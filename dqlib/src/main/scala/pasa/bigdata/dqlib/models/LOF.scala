package pasa.bigdata.dqlib.models

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkException
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param._
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import pasa.bigdata.dqlib.models.CommonUtils.{DEFAULT_ANOMALY_SCORE_COL, DEFAULT_FEATURES_COL, DEFAULT_INDEX_COL}


class LOFModel(override val uid: String) extends Model[LOFModel] with LOFParams {

  private var trainingSummary: Option[LOFSummary] = None

  private var threshold: Double = -1d

  override def copy(extra: ParamMap): LOFModel = {
    val copied = copyValues(new LOFModel(uid), extra)
    copied.setSummary(trainingSummary).setParent(parent)
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def transform(dataSet: Dataset[_]): DataFrame = {
    val session = dataSet.sparkSession
    val sc = session.sparkContext

    // 必须转string，否则报错
    val indexedPointsRDD = dataSet.select(col($(indexCol)).cast(StringType), col($(featuresCol))).rdd.map {
      row => (row.getAs[String](0), row.getAs[Vector](1))
    }
    val numPartitionsOfIndexedPointsRDD = indexedPointsRDD.getNumPartitions

    // compute k-distance neighborhood of each point
    val neighborhoodRDD = Range(0, numPartitionsOfIndexedPointsRDD).map { outId: Int =>
      val outPart = indexedPointsRDD.mapPartitionsWithIndex { (inId, iter) =>
        if (inId == outId) {
          iter
        } else {
          Iterator.empty
        }
      }.collect()
      val bcOutPart = sc.broadcast(outPart)
      indexedPointsRDD.mapPartitions { inPart =>
        val part = inPart.toArray
        val buf = new ArrayBuffer[(String, Array[(String, Double)])]()
        bcOutPart.value.foreach { case (idx: String, vec: Vector) =>
          val neighborhood = computeKNeighborDistance(part, vec, $(minPts), $(distType))
          buf.append((idx, neighborhood))
        }
        buf.iterator
      }.reduceByKey(combineNeighbor)
    }.reduce(_.union(_))

    // 交换顺序，由o结点找到所有以它为近邻的p结点，添加自身后面会用到
    val swappedRDD = neighborhoodRDD.flatMap {
      case (pIndex: String, neighborhood: Array[(String, Double)]) =>
        neighborhood.map { case (oIndex: String, dist: Double) =>
          (oIndex, (pIndex, dist))
        } :+ (pIndex, (pIndex, 0d))
    }.groupByKey().persist()

    val lrdRdd = swappedRDD.cogroup(neighborhoodRDD)
      .flatMap { case (oIdx: String,
      (k: Iterable[Iterable[(String, Double)]], v: Iterable[Array[(String, Double)]])) =>
        // k中存储所有以o为近邻的结点，v中存储了o的k个近邻的距离，最后一个是o的k-distance
        require(k.size == 1 && v.size == 1)
        val kDistance = v.head.last._2 // last is the kth nearest point, i.e. k-distance
        k.head.filter(_._1 != oIdx).map { case (pIdx: String, dist: Double) =>
          (pIdx, (oIdx, Math.max(dist, kDistance))) // p到o的可达距离
        }
      }.groupByKey().map { case (idx: String, iterator: Iterable[(String, Double)]) =>
      val num = iterator.size
      val sum = iterator.map(_._2).sum
      (idx, num / sum) // lrd
    }

    val lofRdd = lrdRdd.cogroup(swappedRDD).flatMap {
      case (oIndex: String, (k: Iterable[Double], v: Iterable[Iterable[(String, Double)]])) =>
        require(k.size == 1 && v.size == 1)
        val lrd = k.head
        v.head.map { case (pIndex: String, _) =>
          // 因为swappedRdd带自身的映射，因此此处也有p到p的lrd
          (pIndex, (oIndex, lrd))
        }
    }.groupByKey().map { case (pIndex: String, iterator: Iterable[(String, Double)]) =>
      require(iterator.exists(_._1 == pIndex))
      val lrd = iterator.find(_._1 == pIndex).get._2
      val sum = iterator.filter(_._1 != pIndex).map(_._2).sum
      (pIndex, sum / lrd / (iterator.size - 1))
    }

    val tmpSchema = StructType(List(
      StructField($(indexCol), StringType, nullable = false),
      StructField($(lofCol), DoubleType, nullable = false)
    ))
    val indexColType = dataSet.schema($(indexCol)).dataType
    val lofDF = session.createDataFrame(lofRdd.map(row => Row(row._1, row._2)), tmpSchema)
      .withColumn($(indexCol), col($(indexCol)).cast(indexColType))
    dataSet.join(lofDF, $(indexCol))
  }

  def hasSummary: Boolean = trainingSummary.nonEmpty

  def setSummary(summary: Option[LOFSummary]): this.type = {
    this.trainingSummary = summary
    this
  }

  def summary: LOFSummary = trainingSummary.getOrElse {
    throw new SparkException(
      s"No training summary available for the ${this.getClass.getSimpleName}"
    )
  }

  def setThreshold(value: Double): this.type = {
    this.threshold = value
    this
  }

  /**
    * Compute k nearest neighbors' distances
    *
    * @param partition the current partition of points
    * @param target    target point
    * @param k         parameter k
    * @param distType  default
    * @return
    */
  def computeKNeighborDistance(partition: Array[(String, Vector)], target: Vector, k: Int, distType: String)
  : Array[(String, Double)] = {

    def rightShift(data: ArrayBuffer[(String, Double)], start: Int): ArrayBuffer[(String, Double)] = {
      require(start >= 0 && start < data.length)
      if (data.nonEmpty) {
        data.append(data.last)
        var i = data.length - 1
        while (i > start) {
          data(i) = data(i - 1)
          i -= 1
        }
      }
      data
    }

    var idxAndDist = new ArrayBuffer[(String, Double)]()
    var count = 0 // the size of distinct instances
    partition.foreach { case (idx: String, vec: Vector) =>
      val targetDist = distType match {
        case LOFModel.euclidean => math.sqrt(Vectors.sqdist(target, vec))
        case _ => throw new IllegalArgumentException(s"Distance type $distType is not supported now.")
      }
      // targetDist equals zero when computing distance between vec and itself
      if (targetDist > 0d) {
        var i = 0
        var inserted = false
        while (i < idxAndDist.length && !inserted) {
          val dist = idxAndDist(i)._2
          if (targetDist <= dist) {
            if (count < k) {
              idxAndDist = rightShift(idxAndDist, i)
              idxAndDist(i) = (idx, targetDist)
              if (targetDist < dist) {
                count += 1
              }
            } else if (count == k) {
              if (targetDist == dist) {
                idxAndDist = rightShift(idxAndDist, i)
                idxAndDist(i) = (idx, targetDist)
              } else {
                var sameRight = 0
                var j = idxAndDist.length - 1
                while (j > 0 && idxAndDist(j)._2 == idxAndDist(j - 1)._2) {
                  sameRight += 1
                  j -= 1
                }
                if (dist == idxAndDist.last._2) {
                  idxAndDist = idxAndDist.dropRight(sameRight)
                } else {
                  idxAndDist = idxAndDist.dropRight(sameRight + 1)
                  idxAndDist = rightShift(idxAndDist, i)
                }
                idxAndDist(i) = (idx, targetDist)
              }
            } else {
              throw new RuntimeException(s"count($count) should not larger than k($k)")
            }
            inserted = true
          }
          i += 1
        }
        if (count < k && !inserted) {
          idxAndDist.append((idx, targetDist))
          count += 1
        }
      }
    }
    idxAndDist.toArray
  }

  /**
    * Combine neighbor between two partitions with same key(point index)
    *
    * @param first  first partition
    * @param second second partition
    * @return
    */
  def combineNeighbor(first: Array[(String, Double)], second: Array[(String, Double)]): Array[(String, Double)] = {
    var pos1 = 0
    var pos2 = 0
    var count = 0 // count distinct distances
    val combined = new ArrayBuffer[(String, Double)]()

    while (pos1 < first.length && pos2 < second.length && count < $(minPts)) {
      if (first(pos1)._2 == second(pos2)._2) {
        combined.append(first(pos1))
        pos1 += 1
        if (combined.length == 1) {
          count += 1
        } else {
          if (combined(combined.length - 1) != combined(combined.length - 2)) {
            count += 1
          }
        }
        combined.append(second(pos2))
        pos2 += 1
      } else {
        if (first(pos1)._2 < second(pos2)._2) {
          combined.append(first(pos1))
          pos1 += 1
        } else {
          combined.append(second(pos2))
          pos2 += 1
        }
        if (combined.length == 1) {
          count += 1
        } else {
          if (combined(combined.length - 1) != combined(combined.length - 2)) {
            count += 1
          }
        }
      }
    }

    while (pos1 < first.length && count < $(minPts)) {
      combined.append(first(pos1))
      pos1 += 1
      if (combined.length == 1) {
        count += 1
      } else {
        if (combined(combined.length - 1) != combined(combined.length - 2)) {
          count += 1
        }
      }
    }

    while (pos2 < second.length && count < $(minPts)) {
      combined.append(second(pos2))
      pos2 += 1
      if (combined.length == 1) {
        count += 1
      } else {
        if (combined(combined.length - 1) != combined(combined.length - 2)) {
          count += 1
        }
      }
    }
    combined.toArray
  }

}

object LOFModel {

  val euclidean = "euclidean"

}

class LOF(override val uid: String) extends Estimator[LOFModel] with LOFParams {

  def this() = this(Identifiable.randomUID("LOF"))

  setDefault(
    minPts -> 5,
    distType -> LOFModel.euclidean,
    indexCol -> DEFAULT_INDEX_COL,
    featuresCol -> DEFAULT_FEATURES_COL,
    lofCol -> DEFAULT_ANOMALY_SCORE_COL
  )

  def setColumns(value: Array[String]): this.type = { set(columns, value) }

  def setIndexCol(value: String): this.type = { set(indexCol, value) }

  def setFeaturesCol(value: String): this.type = { set(featuresCol, value) }

  def setLofCol(value: String): this.type = { set(lofCol, value) }

  def setMinPts(value: Int): this.type = { set(minPts, value) }

  def setDistType(value: String): this.type = { set(distType, value) }

  override def copy(extra: ParamMap): LOF = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def fit(dataSet: Dataset[_]): LOFModel = {
    val model = copyValues(new LOFModel(uid).setParent(this))
    var summary: LOFSummary = null
    try {
      val assembler = new VectorAssembler().setInputCols($(columns)).setOutputCol($(featuresCol))
      val featuredDataSet = assembler.transform(dataSet)
      val dataWithScores = model.transform(featuredDataSet)
      summary = new LOFSummary(dataWithScores)
    } catch {
      case e: Exception =>
        summary = new LOFSummary(dataSet.toDF())
        summary.setSuccess(false)
        summary.setMessage(e.getMessage)
    }
    model.setSummary(Some(summary))
    model
  }

}

trait LOFParams extends Params {

  final val columns: StringArrayParam = new StringArrayParam(parent = this, name = "columns",
    doc = "columns of all features")

  def getColumns: Array[String] = $(columns)

  final val indexCol: Param[String] = new Param[String](parent = this, name = "indexCol", "column of index")

  def getIndexCol: String = $(indexCol)

  final val featuresCol: Param[String] = new Param[String](parent = this, name = "featuresCol",
    doc = "column of features vector")

  def getFeaturesCol: String = $(featuresCol)

  final val lofCol: Param[String] = new Param[String](parent = this, name = "lofCol", doc = "column of lof")

  def getLofCol: String = $(lofCol)

  final val minPts = new IntParam(parent = this, name = "minPts", doc = "minimum number of points",
    ParamValidators.gt(lowerBound = 0))

  def getMinPts: Int = $(minPts)

  final val distType = new Param[String](parent = this, name = "distType", doc = "distance type")

  def getDistType: String = $(distType)

}

class LOFSummary(@transient val dataWithScores: DataFrame) extends CommonSummary with Serializable {

}

