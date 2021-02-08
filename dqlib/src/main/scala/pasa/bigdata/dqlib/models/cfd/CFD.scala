package pasa.bigdata.dqlib.models.cfd

/**
  * Conditional functional dependency
  * @param fd       a standard FD
  * @param tableau  pattern table
  * @param priority priority for more reliable repair
  */
case class CFD(fd: FD, tableau: Array[Pattern], priority: Int) {}
