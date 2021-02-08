package pasa.bigdata.dqlib.models.entities.structures

/***
  * Attribute and its value
  * @param key    attribute
  * @param value  value
  */
case class KeyValue(key: String, value: String) extends Serializable {}
