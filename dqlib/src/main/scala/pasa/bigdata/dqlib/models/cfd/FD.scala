package pasa.bigdata.dqlib.models.cfd

/**
  * Standard functional dependency
  * @param lhs  column indexes of left hand sides
  * @param rhs  column indexes of right hand sides
  */
case class FD(lhs: Array[Int], rhs: Array[Int]) {}
