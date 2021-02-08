package pasa.bigdata.dqlib.models


trait CommonSummary {
  var success: Boolean = true
  var message: String = _

  def setSuccess(suc: Boolean): Unit = { success = suc }
  def setMessage(mess: String): Unit = { message = mess }
}
