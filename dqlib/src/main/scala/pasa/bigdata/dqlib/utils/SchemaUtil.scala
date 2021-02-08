package pasa.bigdata.dqlib.utils

import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

object SchemaUtil {

  def checkColumnType(schema: StructType, colName: String, dataType: DataType, msg: String = ""): Unit = {
    val actualDataType = schema(colName).dataType
    val message = if (msg != null && msg.trim.length > 0) " " + msg else ""
    require(actualDataType.equals(dataType), s"Column $colName must be of type ${dataType.catalogString} but was " +
      s"actually ${actualDataType.catalogString}.$message")
  }

  def appendColumn(schema: StructType, colName: String, dataType: DataType, nullable: Boolean = false): StructType = {
    if (colName.isEmpty) return schema
    appendColumn(schema, StructField(colName, dataType, nullable))
  }

  def appendColumn(schema: StructType, field: StructField): StructType = {
    require(!schema.fieldNames.contains(field.name), s"Column ${field.name} already exists.")
    StructType(schema.fields :+ field)
  }

  /**
    * Generate temporary schema with StringType for creating dataframe and later casting types to target schema
    * @param schema  target schema
    * @return
    */
  def genTmpSchema(schema: StructType): StructType = {
    StructType(schema.fields.map(f => StructField(f.name, StringType, f.nullable)).toList)
  }

}
