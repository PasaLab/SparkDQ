package pasa.bigdata.dqlib.models.imputation

import scala.collection.mutable

/**
  * Attribute value information, storing indexes of null rows and not-null rows for each dependent attribute value
  *
  * @param flag:        true if there exists rows whose target column if null
  * @param nullRows:    indexes of rows whose target column is null
  * @param notNullRows: indexes of rows whose target column is not null
  */
class ValueInformation(var flag: Boolean = false,
                       val nullRows: mutable.HashSet[String] = mutable.HashSet.empty[String],
                       val notNullRows: mutable.HashSet[String] = mutable.HashSet.empty[String]) {

}
