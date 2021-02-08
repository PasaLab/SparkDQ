package pasa.bigdata.dqlib.models.entities.building

object BlockingUtils {

  // nonWord and _
  val DEFAULT_TOKEN_SEPARATOR = "[\\W_]"
  val TOKEN_KEY_SEPARATOR = "_"

  /**
    * Associate tokens to each profile
    * @param profileTokens  profile and its tokens
    * @return
    */
  def associateTokensToProfileID(profileTokens: (Long, Iterable[String])): Iterable[(String, Long)] = {
    val profileID = profileTokens._1
    val tokens = profileTokens._2
    tokens.map(token => (token, profileID))
  }

}
