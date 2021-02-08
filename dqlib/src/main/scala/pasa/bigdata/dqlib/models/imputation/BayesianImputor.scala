package pasa.bigdata.dqlib.models.imputation

import scala.collection.mutable
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import pasa.bigdata.dqlib.models.CommonSummary
import pasa.bigdata.dqlib.models.CommonUtils.DEFAULT_INDEX_COL


class BayesianImputorModel(override val uid: String) extends Model[BayesianImputorModel] with BayesianImputorParams {

  private var trainingSummary: Option[BayesianImputorSummary] = None

  private var imputeMap: scala.collection.Map[String, String] = _

  def getImputeMap: scala.collection.Map[String, String] = imputeMap

  def hasSummary: Boolean = trainingSummary.nonEmpty

  def summary: BayesianImputorSummary = trainingSummary.getOrElse(
    throw new SparkException(s"No training summary available for the ${this.getClass.getSimpleName}")
  )

  private[imputation] def setSummary(summary: Option[BayesianImputorSummary]): this.type = {
    this.trainingSummary = summary
    this
  }

  override def copy(extra: ParamMap): BayesianImputorModel = {
    val copied = copyValues(new BayesianImputorModel(uid), extra)
    copied.setSummary(trainingSummary).setParent(parent)
  }

  override def transformSchema(schema: StructType): StructType = schema

  override def transform(dataSet: Dataset[_]): DataFrame = {
    val spark = dataSet.sparkSession
    val sc = spark.sparkContext
    val indexColIdx = dataSet.columns.indexOf($(indexCol))
    val targetColIdx = dataSet.columns.indexOf($(targetCol))
    val bcDepColIdxs = sc.broadcast[Array[Int]]($(dependentColumns).map(c => dataSet.columns.indexOf(c)))
    val bcAllowedValues = sc.broadcast($(allowedValues))
    val sourceDF = dataSet.toDF()

    // 每种取值在依赖属性的条件下的取值概率
    val likelihoodRDD = sourceDF.rdd.flatMap(row => {
      val id = row(indexColIdx)
      val targetValue = row(targetColIdx)
      val depAttrs = bcDepColIdxs.value
      if (targetValue != null) {
        depAttrs.map(attr => {
          if (row(attr) != null) {
            /**
              * Dependency information format
              *
              * Key:      value of target column(not null)
              * Value:    1. null flag(0: not null, 1: null)
              *           2. id
              *           3. dependent attribute index
              *           4. dependent attribute value
              */
            (targetValue.toString, (0, id.toString, attr, row(attr).toString))
          } else {
            null
          }
        }).filter(x => x != null)
      } else {
        val allowedValues = bcAllowedValues.value
        allowedValues.flatMap(possibleValue => {
          depAttrs.map(attr => {
            if (row(attr) != null) {
              /**
                * Dependency information format
                *
                * Key:      possible value of target column(null)
                * Value:    1. null flag: 1
                *           2. id
                *           3. dependent attribute index
                *           4. dependent attribute value
                */
              (possibleValue, (1, id.toString, attr, row(attr).toString))
            } else {
              null
            }
          }).filter(x => x != null)
        })
      }
    }).groupByKey.flatMap { case (value, infoes) =>
      val likelihood = new mutable.HashMap[(Int, String), ValueInformation]()
      val notNullRows = new mutable.HashSet[String]()
      infoes.foreach(info => {
        val nullFlag = info._1
        val id = info._2
        val depAttrIdx = info._3
        val depAttrVal = info._4
        val depInfo = (depAttrIdx, depAttrVal)
        if (nullFlag == 0) {
          // 当前目标属性取值不为空
          notNullRows.add(id)
          if (!likelihood.contains(depInfo)) {
            likelihood.put(depInfo, new ValueInformation())
          }
          likelihood(depInfo).notNullRows.add(id)
        } else {
          // 当前目标属性取值为空
          if (!likelihood.contains(depInfo)) {
            likelihood.put(depInfo, new ValueInformation())
          }
          likelihood(depInfo).nullRows.add(id)
          likelihood(depInfo).flag = true
        }
      })

      val totalNotNullRows = notNullRows.size
      likelihood.keySet.flatMap(key => {
        // key是形如(依赖属性索引, 依赖属性取值)的元组
        // 该概率表示：当依赖属性及其取值为key时，目标属性取value的概率
        var p: Double = likelihood(key).notNullRows.size
        if (totalNotNullRows > 0) {
          p = p / totalNotNullRows
          if (p == 0.0) {
            p = 1 / totalNotNullRows
          }
        }

        // 目标属性不为空，用于统计概率
        if (likelihood(key).flag) {
          likelihood(key).nullRows.map(id => {
            /**
              * Probability information format
              *
              * Key:      id
              * Value:    1. value of target column
              *           2. probability
              *           3. total number of rows whose target column is not null
              */
            (id, (value, p, totalNotNullRows))
          })
        } else {
          Iterator.empty
        }
      })
    }
    // 求每个目标属性取值为空的行取值概率最大的填充值
    val imputeMap = likelihoodRDD.groupByKey.map { case(id, probs) =>
      val imputeProbs = new mutable.HashMap[String, Double]()
      val imputeCount = new mutable.HashMap[String, Int]()
      probs.foreach(prob => {
        val possibleValue = prob._1
        val p = prob._2
        if (imputeProbs.contains(possibleValue)) {
          imputeProbs.put(possibleValue, imputeProbs(possibleValue) * p)
          imputeCount.put(possibleValue, imputeCount(possibleValue) + 1)
        } else {
          val totalNotNullRows = prob._3
          imputeProbs.put(possibleValue, totalNotNullRows * p)
          imputeCount.put(possibleValue, 1)
        }
      })

      var max: Double = 0.0
      var imputeValue: String = null
      var maxSize: Int = 0
      imputeProbs.keySet.foreach(k => {
        if (imputeCount(k) >= maxSize) {
          maxSize = imputeCount(k)
          if (max < imputeProbs(k)) {
            max = imputeProbs(k)
            imputeValue = k
          }
        }
      })
      (id, imputeValue)
    }.collectAsMap()
    this.imputeMap = imputeMap

    val bcImputeMap = sc.broadcast(imputeMap)
    val imputedRDD = sourceDF.rdd.map(row => {
      if (row(targetColIdx) == null) {
        val s: Array[Any] = row.toSeq.toArray.updated(targetColIdx, bcImputeMap.value(row(indexColIdx).toString))
        new GenericRowWithSchema(s, row.schema)
      } else {
        row
      }
    })
    spark.createDataFrame(imputedRDD, dataSet.schema)
  }

}

class BayesianImputor(override val uid: String) extends Estimator[BayesianImputorModel] with BayesianImputorParams {

  def this() = this(Identifiable.randomUID("BayesianImputor"))

  setDefault(
    allowedValues -> Array.empty,
    indexCol -> DEFAULT_INDEX_COL
  )

  def setIndexCol(value: String): this.type = set(indexCol, value)

  def setTargetCol(value: String): this.type = set(targetCol, value)

  def setDependentColumns(value: Array[String]): this.type = set(dependentColumns, value)

  def setAllowedValues(value: Array[String]): this.type = set(allowedValues, value)

  override def copy(extra: ParamMap): Estimator[BayesianImputorModel] = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = schema

  override def fit(dataSet: Dataset[_]): BayesianImputorModel = {
    // reset allowedValues if empty
    if ($(allowedValues).isEmpty) {
      val distinctValues: Array[String] = dataSet.select(col($(targetCol)).cast(StringType))
        .na.drop
        .distinct
        .collect
        .map(x => x.getAs[String](0))
      setAllowedValues(distinctValues)
    }

    var summary: BayesianImputorSummary = null
    val model = copyValues(new BayesianImputorModel(uid).setParent(this))
    try {
      summary = new BayesianImputorSummary(model.transform(dataSet), model.getImputeMap)
    } catch {
      case e: Exception =>
        summary = new BayesianImputorSummary(dataSet.toDF(), new scala.collection.immutable.HashMap[String, String]())
        summary.setSuccess(false)
        summary.setMessage(e.getMessage)
    }
    model.setSummary(Some(summary))
    model
  }

}

object BayesianImputor {}

trait BayesianImputorParams extends Params {

  final val indexCol: Param[String] = new Param[String](parent = this, name = "indexCol", doc = "column of index")

  def getIndexCol: String = $(indexCol)

  final val targetCol: Param[String] = new Param[String](parent = this, name = "targetCol",
    doc = "target column for imputing")

  def getTargetCol: String = $(targetCol)

  final val dependentColumns: StringArrayParam = new StringArrayParam(parent = this, name = "dependentColumns",
    doc = "dependent columns of target column")

  def getDependentColumns: Array[String] = $(dependentColumns)

  final val allowedValues: StringArrayParam = new StringArrayParam(parent = this, name = "allowedValues",
    doc = "allowed values for imputing")

  def getAllowedValues: Array[String] = $(allowedValues)

}

class BayesianImputorSummary(@transient val imputedData: DataFrame,
                             val imputeMap: scala.collection.Map[String, String])
  extends CommonSummary
  with Serializable {
}
