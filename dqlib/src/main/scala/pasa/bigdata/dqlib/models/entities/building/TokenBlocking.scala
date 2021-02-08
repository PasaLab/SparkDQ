package pasa.bigdata.dqlib.models.entities.building

import org.apache.spark.rdd.RDD

import pasa.bigdata.dqlib.models.entities.structures._
import pasa.bigdata.dqlib.models.entities.building.BlockingUtils.{DEFAULT_TOKEN_SEPARATOR, TOKEN_KEY_SEPARATOR}

object TokenBlocking {

  /**
    * Create blocks from profiles
    * @param profiles profiles
    * @param createTokensFunctions functions for creating tokens
    * @return
    */
  def createBlocks(profiles: RDD[Profile],
                   createTokensFunctions: Iterable[KeyValue] => Iterable[String] =
                   createTokensFromProfileAttributes): RDD[BlockAbstract] = {
    val tokensPerProfile = profiles.map(profile => (profile.profileID,
      createTokensFunctions(profile.attributes).toSet))
    val profilesPerKey = tokensPerProfile.flatMap(BlockingUtils.associateTokensToProfileID).groupByKey()
      .filter(_._2.size > 1)
    val profilesPerBlock = profilesPerKey.map(token => {
      val profileIDs = token._2.toSet
      Array(profileIDs)
    }).filter(_.head.size > 1).zipWithIndex()
    profilesPerBlock.map {
      case (profileIDs, blockID) => BlockDirty(blockID, profileIDs)
    }
  }

  /**
    * Create blocks from profiles according to attribute clusters
    * @param profiles               profiles
    * @param clusters               clusters as (ClusterID, keys, entropy)
    * @param excludeDefault         if exclude default cluster
    * @return
    */
  def createBlocksCluster(profiles: RDD[Profile], clusters: List[AttributesCluster],
                          excludeDefault: Boolean = false): RDD[BlockAbstract] = {
    val defaultClusterID = clusters.maxBy(_.clusterID).clusterID
    val entropies = clusters.map(cluster => (cluster.clusterID, cluster.entropy)).toMap
    val clusterMap = clusters.flatMap(cluster => cluster.attributes.map(attr => (attr, cluster.clusterID))).toMap

    val tokensPerProfile = profiles.map {
      profile =>
        val tokens = profile.attributes.flatMap { kv =>
          val clusterID = clusterMap.getOrElse(kv.key, defaultClusterID)
          val values = kv.value.split(DEFAULT_TOKEN_SEPARATOR).map(_.toLowerCase.trim)
            .filter(_.nonEmpty).distinct
          values.map(_ + TOKEN_KEY_SEPARATOR + clusterID)
        }
        (profile.profileID, tokens.distinct)
    }

    // group profiles by token key
    val profilesPerTokenKey = {
      if (excludeDefault) {
        tokensPerProfile.flatMap(BlockingUtils.associateTokensToProfileID).groupByKey()
          .filter(!_._1.endsWith(TOKEN_KEY_SEPARATOR + defaultClusterID))
      } else {
        tokensPerProfile.flatMap(BlockingUtils.associateTokensToProfileID).groupByKey()
      }
    }
    val profilesPerBlock = profilesPerTokenKey.map {
      case (tokenKey, profileIDs) =>
        val profiles = Array(profileIDs.toSet)
        var clusterID = defaultClusterID
        val entropy = {
          clusterID = tokenKey.split(TOKEN_KEY_SEPARATOR).last.toInt
          val e = entropies.get(clusterID)
          if (e.isDefined) {
            e.get
          } else {
            0.0
          }
        }
        (profiles, entropy, clusterID, tokenKey)
    }.filter(_._1.head.size > 1).zipWithIndex()
    profilesPerBlock.map { case ((blockProfiles, entropy, clusterID, blockingKey), blockID) =>
      BlockDirty(blockID, blockProfiles, entropy, clusterID, blockingKey)
    }
  }

  /**
    * Create tokens from attributes
    */
  def createTokensFromProfileAttributes(attributes: Iterable[KeyValue]): Iterable[String] = {
    attributes.map(attribute => attribute.value.toLowerCase)
      .filter(_.trim.length > 0)
      .flatMap(value => value.split(DEFAULT_TOKEN_SEPARATOR))
      .filter(_.trim.length > 0)
  }

  /**
    * separate profiles by separators of all datasets
    * @param elements all profiles
    * @param separators separators of all datasets
    * @return
    */
  def separateProfiles(elements: Set[Long], separators: Array[Long]): Array[Set[Long]] = {
    var left = elements
    var parts: List[Set[Long]] = Nil
    separators.foreach {
      sep =>
        val tmp = left.partition(_ <= sep)
        left = tmp._2
        parts = tmp._1 :: parts
    }
    parts = left :: parts
    parts.reverse.toArray
  }

}
