package pasa.bigdata.dqlib.models.entities.structures


/**
  * Profile represents one piece of data
 *
  * @param profileID    profile ID
  * @param attributes   attributes of [[KeyValue]]
  * @param realID       real ID of data
  */
case class Profile(profileID: Long, attributes: scala.collection.mutable.MutableList[KeyValue] =
new scala.collection.mutable.MutableList(), realID: String = "") extends Serializable {

  def addAttribute(a: KeyValue): Unit = {
    attributes += a
  }

  def getAttributeValue(key: String, separator: String = " "): String = {
    attributes.filter(attr => attr.key.equals(key)).map(_.value).mkString(separator)
  }

  /**
    * return attributes map, merge attribute values with blank
    * @return
    */
  def toAttributesMap: Map[String, String] = {
    attributes.map(kv => (kv.key, kv.value)).groupBy(_._1).map {
      case (attribute, values) =>
        (attribute, values.map(x => x._2).mkString(" "))
    }.toList.toMap
  }

  /**
    * return attributes count map, each attribute has a value map
    * @return
    */
  def toAttributesCountMap: Map[String, Map[String, Int]] = {
    attributes.map(kv => (kv.key, kv.value)).groupBy(_._1).map {
      case (attribute, values) =>
        val valuesMap = values.groupBy(_._1).map(x => (x._1, x._2.size)).toList.toMap
        (attribute, valuesMap)
    }.toList.toMap
  }

}