package pasa.bigdata.dqlib.models.entities.structures

/**
  * Attributes cluster
  * @param clusterID      cluster id
  * @param attributes     attribute names
  * @param entropy        entropy of this cluster
  */
case class AttributesCluster(clusterID: Int, attributes: List[String], entropy: Double = 0.0) {}
