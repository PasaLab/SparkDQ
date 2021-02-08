package pasa.bigdata.dqlib.models.entities.structures

/**
  * Candidate entities
  * @param firstProfileID         first profile ID
  * @param secondProfileID        second profile ID
  */
case class UnweightedEdge(firstProfileID: Long, secondProfileID: Long) extends EdgeTrait with Serializable {}
