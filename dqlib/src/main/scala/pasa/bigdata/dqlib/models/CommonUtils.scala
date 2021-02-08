package pasa.bigdata.dqlib.models

object CommonUtils {

  // default column names
  val DEFAULT_INDEX_COL: String = "id"
  val DEFAULT_FEATURES_COL: String = "features"
  val DEFAULT_ANOMALY_SCORE_COL: String = "anomaly_score"
  val DEFAULT_CLUSTER_COL: String = "cluster"

  // default values
  val DEFAULT_SEPARATOR: String = ","

  object TaskTypes {
    val DETECT = "detect"
    val REPAIR = "repair"
  }

  object SimilarityTypes {
    val COSINE = "cosine"
    val JARO_WINKLER = "jaro-winkler"
    val JACCARD_INDEX = "jaccard index"
    val NORMALIZED_LEVENSHTEIN = "normalized levenshtein"
    val SORENSEN_DICE_COEFFICIENT = "sorensen-dice coefficient"
  }

}
