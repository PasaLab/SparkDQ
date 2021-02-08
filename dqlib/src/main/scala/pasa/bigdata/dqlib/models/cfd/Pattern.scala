package pasa.bigdata.dqlib.models.cfd

/**
  * Pattern tableau consists of left and rights values of FD
  * @param left   left values of LHS
  * @param right  right values of RHS
  */
case class Pattern(left: Array[String], right: Array[String]) {}
