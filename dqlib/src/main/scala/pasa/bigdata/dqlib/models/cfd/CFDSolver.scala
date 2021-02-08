package pasa.bigdata.dqlib.models.cfd

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.StructType

import pasa.bigdata.dqlib.models.CommonUtils.{DEFAULT_INDEX_COL, DEFAULT_SEPARATOR, TaskTypes}
import pasa.bigdata.dqlib.models.CommonSummary
import pasa.bigdata.dqlib.utils.SchemaUtil


class CFDSolverModel(override val uid: String) extends Model[CFDSolverModel] with CFDSolverParams {

  var success: Boolean = true
  var message: String = _

  def setSuccess(suc: Boolean): Unit = { success = suc }
  def setMessage(mess: String): Unit = { message = mess }

  private[cfd] var violations: Map[Int, Array[Array[(Int, Int)]]] = _

  private var trainingSummary: Option[CFDSolverSummary] = None

  private[cfd] def setSummary(summary: Option[CFDSolverSummary]): this.type = {
    this.trainingSummary = summary
    this
  }

  def hasSummary: Boolean = trainingSummary.nonEmpty

  def summary: CFDSolverSummary = trainingSummary.getOrElse(
    throw new SparkException(s"No training summary available for the ${this.getClass.getSimpleName}")
  )

  override def copy(extra: ParamMap): CFDSolverModel = {
    val copied = copyValues(new CFDSolverModel(uid), extra)
    copied.setSummary(trainingSummary).setParent(parent)
  }

  override def transformSchema(schema: StructType): StructType = schema

  override def transform(dataSet: Dataset[_]): DataFrame = {

    try {
      val fieldNames = dataSet.schema.fieldNames
      val arrayCFDs: Array[CFD] = CFDSolverModel.parseCFDs($(cfds), $(priorities), fieldNames)
      val idxCol: Int = fieldNames.indexOf($(indexCol))

      val rdd = dataSet.toDF.rdd
      if ($(taskType).equals(TaskTypes.DETECT)) {
        val data = rdd.map(row => row.toSeq.toArray.map(x => if (x != null) x.toString else null))
        violations = detectViolations(data, arrayCFDs, idxCol)
        dataSet.toDF
      } else if ($(taskType).equals(TaskTypes.REPAIR)) {
        if (CFDSolverModel.checkCFDs(arrayCFDs)) {
          val spark = dataSet.sparkSession
          val sc = spark.sparkContext
          val bcCFDs = sc.broadcast(arrayCFDs)
          var data = rdd.map(row => row.toSeq.toArray.map(x => if (x != null) x.toString else null))

          var round = 0
          var over = false
          while ((round < 3) && !over) {
            // 1. repair constant violations and check potential variable violations
            val groupedRdd = data.flatMap { row =>
              // if null, cannot call toString function
              val id = row(idxCol)
              val inCFDs = bcCFDs.value

              // priorities of attributes in RHS
              val prioMap: scala.collection.mutable.HashMap[Int, Int] = scala.collection.mutable.HashMap.empty[Int, Int]
              inCFDs.foreach { cfd =>
                val lhs = cfd.fd.lhs
                val rhs = cfd.fd.rhs
                val patterns = cfd.tableau
                patterns.foreach { pattern =>
                  // LHS matches
                  if (lhs.zip(pattern.left).forall(p => CFDSolverModel.matchPattern(row(p._1), p._2))) {
                    rhs.zip(pattern.right).foreach{ case (colIdx, value) =>
                      if (!value.equals("_") && !CFDSolverModel.matchPattern(row(colIdx), value)) {
                        if (!prioMap.contains(colIdx) || (cfd.priority >= prioMap(colIdx))) {
                          prioMap.update(colIdx, cfd.priority)
                          row.update(colIdx, value)
                        }
                      }
                    }
                  }
                }
              }

              // check potential variable violations
              val groupedTuple = inCFDs.zipWithIndex.flatMap { case (cfd, cfdIdx) =>
                val lhs = cfd.fd.lhs
                val rhs = cfd.fd.rhs
                val patterns = cfd.tableau
                patterns.zipWithIndex.flatMap { case (pattern, patIdx) =>
                  // LHS matches
                  if (lhs.zip(pattern.left).forall(p => CFDSolverModel.matchPattern(row(p._1), p._2))) {
                    val lhsValues = lhs.map(i => row(i)).mkString($(separator))
                    rhs.zip(pattern.right).map { case (colIdx, value) =>
                      // RHS matches, if current attribute repaired due to constant violations,
                      // only consider more prior CFD
                      if (value.equals("_") && (!prioMap.contains(colIdx) || (cfd.priority >= prioMap(colIdx)))) {
                        ((cfdIdx, patIdx, lhsValues, colIdx, 0), (id, row(colIdx)))
                      } else {
                        null
                      }
                    }.filter(x => x!= null)
                  } else {
                    Array.empty[((Int, Int, String, Int, Int), (String, String))]
                  }
                }
              }

              /**
                * Group information format
                * Key:    1. CFD index,
                *         2. pattern index,
                *         3. LHS values,
                *         4. column index,
                *         5. group flag(1: row data, 0: potential variable violations)
                * Value:  1. id value,
                *         2. row data string when flag == 1, RHS value when flag == 0
                */
              if (groupedTuple.isEmpty) {
                // no potential variable violations, row data
                Array(((-1, -1, "", -1, 1), (id, row.mkString($(separator)))))
              } else {
                // still need to keep row data
                groupedTuple ++ Array(((-1, -1, "", -1, 1), (id, row.mkString($(separator)))))
              }
            }

            // 2. generate repair plans based on probability of RHS value
            val plannedRdd = groupedRdd.groupByKey.flatMap { case (key, values) =>
              /**
                *  Repair information format
                *  Key:     id value
                *  Value:   1. repair flag(0: row data, 1: repair plan),
                *           2. column index
                *           3. fix value when flag == 1, row data string when flag == 0
                *           4. CFD index
                *           5. pattern index
                */
              val repairPlan =
                if (key._5 == 1) {
                  // row data, only one piece
                  values.map {
                    case (id, tupleStr) =>
                      (id, (0, -1, tupleStr, -1, -1))
                  }
                } else {
                  val cfdIdx = key._1
                  val patIdx= key._2
                  val colIdx = key._4
                  val countMap: scala.collection.mutable.HashMap[String, Int] =
                    scala.collection.mutable.HashMap.empty[String, Int]
                  values.foreach { case (_, value) =>
                    if (countMap.contains(value)) {
                      countMap.update(value, countMap(value) + 1)
                    } else {
                      countMap.update(value, 1)
                    }
                  }

                  if (countMap.size > 1) {
                    // choose the RHS value with highest probability for one LHS values of a pattern in a CFD
                    val fixValue = countMap.maxBy(_._2)._1
                    values.map(x => (x._1, (1, colIdx, fixValue, cfdIdx, patIdx)))
                  } else {
                    Iterator.empty
                  }
                }
              repairPlan
            }

            // 3. repair rdd
            data = plannedRdd.groupByKey.map { case (_, values) =>
              val tuple: Array[String] = values.filter(x => x._1 == 0).head._3.split($(separator))
              val plans = values.filter(x => x._1 == 1)
              if (plans.nonEmpty) {
                val inCFDs = bcCFDs.value
                // Equivalence class format: (colIdx, (fixValue, priority))
                val ec: scala.collection.mutable.HashMap[Int, scala.collection.mutable.HashMap[String, Int]] =
                  scala.collection.mutable.HashMap.empty[Int, scala.collection.mutable.HashMap[String, Int]]
                plans.foreach { plan =>
                  val colIdx = plan._2
                  val fixValue = plan._3
                  val cfdIdx = plan._4
                  val currPrio = inCFDs(cfdIdx).priority
                  if (!ec.contains(colIdx)) {
                    ec.update(colIdx, scala.collection.mutable.HashMap[String, Int]((fixValue, currPrio)))
                  } else {
                    val prePrio = ec(colIdx)(fixValue)
                    ec(colIdx).update(fixValue, currPrio + prePrio)
                  }
                }
                ec.foreach{ case (colIdx, valueMap) =>
                  tuple(colIdx) = valueMap.maxBy(_._2)._1
                }
              }
              tuple
            }
            // if go on
            round += 1
            over = quickDetectViolations(data, arrayCFDs, idxCol)
          }

          val rowRdd = data.map(tuple => Row.fromSeq(tuple))
          val targetSchema = dataSet.schema
          // tmp string schema
          val tmpSchema = SchemaUtil.genTmpSchema(targetSchema)
          // cast string to target types
          spark.createDataFrame(rowRdd, schema = tmpSchema).selectExpr(targetSchema.map(field =>
            s"CAST ( ${field.name} As ${field.dataType.sql}) ${field.name}"): _*)
        } else {
          success = false
          message = "Inconsistent CFDs"
          dataSet.toDF()
        }
      } else {
        success = false
        message = "Invalid task type %s" format $(taskType)
        dataSet.toDF
      }
    } catch {
      case e: Exception =>
        success = false
        message = e.getMessage
        dataSet.toDF
    }

  }

  /**
    * Quickly detect violations for checking whether there are still errors
    * different from detectViolations, need to consider priorities
    * @param data       data for detection
    * @param arrayCFDs  CFDs
    * @param idxCol     index column
    * @return
    */
  def quickDetectViolations(data: RDD[Array[String]], arrayCFDs: Array[CFD], idxCol: Int): Boolean = {
    val bcCFDs = data.sparkContext.broadcast(arrayCFDs)
    // check constant violations and group by LHS for variable violations
    val groupedRdd = data.flatMap { row =>
      val inCFDs = bcCFDs.value
      // constant violations and grouped tuples
      var constVioAndGroup = ArrayBuffer.empty[((Int, String, Int, Int, Int), String)]
      inCFDs.zipWithIndex.foreach { case (cfd, cfdIdx) =>
        val lhs = cfd.fd.lhs
        val rhs = cfd.fd.rhs
        val patterns = cfd.tableau
        patterns.zipWithIndex.foreach { case (pattern, patIdx) =>
          // only when lhs matches, violations may appear
          if (lhs.zip(pattern.left).forall(p => CFDSolverModel.matchPattern(row(p._1), p._2))) {
            constVioAndGroup ++= rhs.zip(pattern.right).map { p =>
              val colIdx = p._1
              val patVal = p._2
              if (!patVal.equals("_") && !CFDSolverModel.matchPattern(row(colIdx), patVal)) {
                ((cfdIdx, "", patIdx, colIdx, 1), row(colIdx))
              } else if (p._2.equals("_")) {
                val lhsValues = lhs.map(i => row(i)).mkString($(separator))
                ((cfdIdx, lhsValues, patIdx, colIdx, 0), row(colIdx))
              } else {
                null
              }
            }.filter(x => x != null)
          }
        }
      }
      constVioAndGroup
    }

    // verify constant and variable violations for each LHS, RHS attribute of every pattern in every CFD
    val verifyRdd = groupedRdd.groupByKey.map { case (key, values) =>
      val vioType = key._5
      if (vioType == 1) {
        // constant violations
        true
      } else {
        // variable violations
        val distinct = values.toArray.distinct
        if (distinct.length > 1) {
          true
        } else {
          false
        }
      }
    }
    // true means existing errors
    !verifyRdd.filter(_ == true).isEmpty()
  }

  /**
    * Detect violations for each CFD, refining to each pattern tuple in a CFD
    * @param data       data for detection
    * @param arrayCFDs  CFDs
    * @param idxCol     index column
    * @return constant and variable violations of each pattern tuple in every CFD
    */
  def detectViolations(data: RDD[Array[String]], arrayCFDs: Array[CFD], idxCol: Int)
  : Map[Int, Array[Array[(Int, Int)]]] = {
    val bcCFDs = data.sparkContext.broadcast(arrayCFDs)
    // check constant violations and group by LHS for variable violations
    val groupedRdd = data.flatMap { row =>
      val inCFDs = bcCFDs.value
      // constant violations and grouped tuples
      var constVioAndGroup = ArrayBuffer.empty[((Int, String, Int, Int, Int), String)]
      inCFDs.zipWithIndex.foreach { case (cfd, cfdIdx) =>
        val lhs = cfd.fd.lhs
        val rhs = cfd.fd.rhs
        val patterns = cfd.tableau
        patterns.zipWithIndex.foreach { case (pattern, patIdx) =>
          // only when lhs matches, violations may appear
          if (lhs.zip(pattern.left).forall(p => CFDSolverModel.matchPattern(row(p._1), p._2))) {
            constVioAndGroup ++= rhs.zip(pattern.right).map { p =>
              val colIdx = p._1
              val patVal = p._2
              if (!patVal.equals("_") && !CFDSolverModel.matchPattern(row(colIdx), patVal)) {
                /**
                  * For X -> A
                  * Constant violations: tp[A] is constant and t[A] != tp[A]
                  *
                  * Inconsistency information format:
                  * Key:    1. CFD index,
                  *         2. "",
                  *         3. pattern index,
                  *         4. column index,
                  *         5. violation type(1: constant, 0: variable)
                  * Value:  RHS value
                  */
                ((cfdIdx, "", patIdx, colIdx, 1), row(colIdx))
              } else if (p._2.equals("_")) {
                /**
                  * potential variable violations: tp[A] is "_"
                  *
                  * Inconsistency information format:
                  * Key:    1. CFD index,
                  *         2. LHS values,
                  *         3. pattern index,
                  *         4. column index,
                  *         5. violation type(1: constant, 0: variable)
                  * Value:  RHS value
                  */
                val lhsValues = lhs.map(i => row(i)).mkString($(separator))
                ((cfdIdx, lhsValues, patIdx, colIdx, 0), row(colIdx))
              } else {
                null
              }
            }.filter(x => x != null)
          }
        }
      }
      constVioAndGroup
    }

    // count constant and variable violations for each LHS, RHS attribute of every pattern in every CFD
    val countRdd = groupedRdd.groupByKey.map { case (key, values) =>
      val cfdIdx = key._1
      val patIdx = key._3
      val colIdx = key._4
      val vioType = key._5
      /**
        * Count information format:
        * 1. CFD index,
        * 2. pattern index,
        * 3. violation type(1: constant, 0: variable),
        * 4. number
        */
      if (vioType == 1) {
        // constant violations
        ((cfdIdx, patIdx, colIdx, vioType), values.size)
      } else {
        // variable violations
        val countMap: scala.collection.mutable.HashMap[String, Int] = scala.collection.mutable.HashMap.empty[String, Int]
        values.foreach { value =>
          if (countMap.contains(value)) {
            countMap.update(value, countMap(value) + 1)
          } else {
            countMap.update(value, 1)
          }
        }
        if (countMap.size > 1) {
          // 假设三种取值，每个集合各有2，3，4条记录，那么总共多少对？
          // 2 * 3 + 3 * 4 + 2 * 4
          val rightAcc = countMap.values.scanRight(0)(_ + _).tail
          val sum = countMap.values.zip(rightAcc).map(x => x._1 * x._2).sum
          ((cfdIdx, patIdx, colIdx, vioType), sum)
        } else {
          ((cfdIdx, patIdx, colIdx, vioType), 0)
        }
      }
    }
    // sum all counts with different LHS values
    val violationsMap = countRdd.groupByKey.map{ case (key, values) => (key, values.sum)}.collectAsMap()
    arrayCFDs.zipWithIndex.map {
      case (cfd, cfdIdx) =>
        val rhs = cfd.fd.rhs
        val patternViolations = cfd.tableau.zipWithIndex.map {
          case (_, patIdx) =>
            rhs.map(colIdx => (
              violationsMap.getOrElse((cfdIdx, patIdx, colIdx, 1), 0),
              violationsMap.getOrElse((cfdIdx, patIdx, colIdx, 0), 0))
            )
        }
        (cfdIdx, patternViolations)
    }.toMap
  }

}

object CFDSolverModel {

  /**
    * Judge if actual value matches pattern value
    * @param actVal  actual value
    * @param patVal  pattern value
    * @return
    */
  def matchPattern(actVal: String, patVal: String): Boolean = {
    patVal.equals("_") || patVal.equals(actVal)
  }

  /**
    * Parse CFD strings to CFD objects
    * @param cfds         CFD strings
    *                     CFD:      FD#pattern1#pattern2...
    *                     FD:       A,B@C
    *                     Pattern:  _,_&_
    * @param fieldNames   data field names
    * @return
    */
  def parseCFDs(cfds: Array[String], priorities: Array[Int], fieldNames: Array[String]): Array[CFD] = {
    cfds.zip(priorities).map { case (cfd, pri) =>
      // FD#patterns
      val fdPats = cfd.split("#")

      // FD
      val fd = fdPats(0).split("@")
      val lhs = fd(0).split(",").map(x => fieldNames.indexOf(x))
      val rhs = fd(1).split(",").map(x => fieldNames.indexOf(x))

      // patterns are from 1 to last
      val pats = fdPats.slice(1, fdPats.length).map { pat =>
        val values = pat.split("&")
        val lhsVals = values(0).split(",")
        val rhsVals = values(1).split(",")
        Pattern(lhsVals, rhsVals)
      }
      CFD(FD(lhs, rhs), pats, pri)
    }
  }

  /**
    * Check whether CFDs are consistent
    * @param cfds  CFDs
    * @return
    */
  def checkCFDs(cfds: Array[CFD]): Boolean = {
    val map = new scala.collection.mutable.HashMap[Int, String]
    cfds.foreach(cfd =>
      cfd.tableau.foreach{pattern =>
        if (pattern.left.forall(v => v.equals("_"))) {
          for (idx <- cfd.fd.rhs.indices) {
            val key = cfd.fd.rhs(idx)
            val value = pattern.right(idx)
            if (!value.equals("_")) {
              if (map.contains(key) && !map(key).equals(value)) {
                return false
              }
              map.update(key, value)
            }
          }
        }
      }
    )

    if (map.isEmpty) return true
    var oldSize = map.size
    var newSize = 0
    while (true) {
      cfds.foreach(cfd =>
        cfd.tableau.foreach{pattern =>
          if (cfd.fd.lhs.zip(pattern.left).exists(x => map.contains(x._1) && map(x._1).equals(x._2))) {
            for (idx <- cfd.fd.rhs.indices) {
              val key = cfd.fd.rhs(idx)
              val value = pattern.right(idx)
              if (!value.equals("_")) {
                if (map.contains(key) && !map(key).equals(value)) {
                  return false
                }
                map.update(key, value)
              }
            }
          }
        }
      )
      newSize = map.size
      if (oldSize != newSize) {
        oldSize = newSize
      } else {
        return true
      }
    }
    true
  }

}

class CFDSolver(override val uid: String) extends Estimator[CFDSolverModel] with CFDSolverParams {

  def this() = this(Identifiable.randomUID("CFDSolver"))

  setDefault(
    indexCol    -> DEFAULT_INDEX_COL,
    separator   -> DEFAULT_SEPARATOR,
    taskType    -> TaskTypes.DETECT,
    priorities  -> Array.empty[Int],
    maxRounds   -> 3
  )

  def setIndexCol(value: String): this.type = set(indexCol, value)

  def setSeparator(value: String): this.type = set(separator, value)

  def setCFDs(value: Array[String]): this.type = set(cfds, value)

  def setPriorities(value: Array[Int]): this.type = set(priorities, value)

  def setTaskType(value: String): this.type = set(taskType, value)

  def setMaxRounds(value: Int): this.type = set(maxRounds, value)

  override def copy(extra: ParamMap): CFDSolver = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = schema

  override def fit(dataset: Dataset[_]): CFDSolverModel = {
    // if not set, equally handle CFDs
    if ($(priorities).isEmpty) {
      setPriorities(Array.fill[Int]($(cfds).length)(0))
    }
    val model = copyValues(new CFDSolverModel(uid).setParent(this))
    val summary = new CFDSolverSummary(model.transform(dataset))
    summary.setViolations(model.violations)
    summary.setSuccess(model.success)
    summary.setMessage(model.message)
    model.setSummary(Some(summary))
    model
  }

}

object CFDSolver {}

trait CFDSolverParams extends Params {

  final val indexCol: Param[String] = new Param[String](parent = this, name = "indexCol", doc = "index column")

  final val separator: Param[String] = new Param[String](parent = this, name = "separator", doc = "separator for LHS " +
    "values")

  def getIndexCol: String = $(indexCol)

  final val cfds: StringArrayParam = new StringArrayParam(parent = this, name = "cfds",
    doc = "CFD|pattern1|pattern2...")

  def getCFDs: Array[String] = $(cfds)

  final val priorities: IntArrayParam = new IntArrayParam(parent = this, name = "priorities", doc = "priorities of " +
    "CFDs")

  def getPriorities: Array[Int] = $(priorities)

  final val taskType: Param[String] = new Param[String](parent = this, name = "taskType", doc = "task type")

  def getTaskType: String = $(taskType)

  final val maxRounds: Param[Int] = new Param[Int](parent = this, name = "maxRounds", doc = "maximum repairing rounds")

  def getMaxRounds: Int = $(maxRounds)

}

class CFDSolverSummary(@transient val targetData: DataFrame) extends CommonSummary  with Serializable {

  var violations: String = ""

  def setViolations(vios: Map[Int, Array[Array[(Int, Int)]]]): Unit = {
    if (vios != null) {
      violations = vios.toList.sortBy(_._1).map(cfdVios => {
        cfdVios._2.map(patVios => {
          patVios.map(attrVios => "%d,%d".format(attrVios._1, attrVios._2)).mkString(";")
        }).mkString("/")
      }).mkString("#")
    }
  }

}