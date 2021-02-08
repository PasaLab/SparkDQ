package pasa.bigdata.dqlib.models.entities.building

import scala.util.Random

import org.apache.spark.rdd.RDD
import org.jgrapht.alg.connectivity.ConnectivityInspector
import org.jgrapht.graph.{DefaultEdge, SimpleGraph}

import pasa.bigdata.dqlib.models.entities.structures.{AttributesCluster, Profile}


/**s
  * Locality sensitive hashings
  */
object LSH {

  val SOURCE_NAME_SEPARATOR = "_"

  def getHashes(startHash: Int, numHashes: Int, seed: Int = 1234): Array[Int] = {
    val rnd = new Random(seed)
    val hashes = for (_ <- 0 until numHashes) yield {
      val a = rnd.nextInt() + 1
      val b = rnd.nextInt()
      (((a.toLong * startHash.toLong + b.toLong) % 2147495899L) % Integer.MAX_VALUE).toInt
    }
    hashes.toArray
  }

  /**
    * Get number of rows for each band
    * @param targetThreshold  target threshold for computing number of bands
    * @param sigNum           number of signature values
    * @return
    */
  def getNumRows(targetThreshold: Double, sigNum: Int): Int = {
    val bands = getNumBands(targetThreshold, sigNum)
    val nRows = sigNum / bands
    if (nRows < 1) {
      1
    } else {
      nRows
    }
  }

  /**
    * Get number of bands according to the assigned threshold
    * @param targetThreshold    compare with (1/b)^^(1/r)
    * @param sigNum             number of signature values
    * @return
    */
  def getNumBands(targetThreshold: Double, sigNum: Int): Int = {
    var b = sigNum
    def r: Double = sigNum.toDouble / b
    def t: Double = Math.pow(1.0 / b, 1.0 / r)
    while (t < targetThreshold && b > 1) {
      b -= 1
    }
    b + 1
  }

  /**
    * Count how many pairs of min-hash values equals
    * @param sig1 signature1
    * @param sig2 signature2
    * @return
    */
  def calcSimilarity(sig1: Array[Int], sig2: Array[Int]): Double = {
    var equals: Double = 0
    for (i <- sig1.indices if sig1(i) == sig2(i)) {
      equals += 1
    }
    equals / sig1.length.toDouble
  }

  /**
    * Cluster similar attributes and compute entropies of each cluster
    * @param profiles           input profiles
    * @param numHashes          number of min-hash values as signature (equal to number of hash functions)
    * @param targetThreshold    similarity target threshold for computing band size (larger band size, less computing
    *                           cost)
    * @param maxFactor          maxFactor to remove edges
    * @param numBands           number of bands(not used)
    * @param computeEntropy     if compute entropy
    * @return
    */
  def clusterSimilarAttributes(profiles: RDD[Profile],
                               numHashes: Int,
                               targetThreshold: Double,
                               maxFactor: Double,
                               numBands: Int = -1,
                               computeEntropy: Boolean = false)
  : List[AttributesCluster] = {
    // generate tokens
    val attributesToken: RDD[(String, String)] = profiles.flatMap {
      profile => {
        val attributes = profile.attributes
        attributes.flatMap { kv =>
          kv.value.split(BlockingUtils.DEFAULT_TOKEN_SEPARATOR).filter(_.trim.nonEmpty).map(_.toLowerCase)
            .map(token => (kv.key, token))
        }
      }
    }

    val attributesPerToken: RDD[(Int, Iterable[String])] = attributesToken.map(_.swap).groupByKey().zipWithIndex()
      .map(x => (x._2.toInt, x._1._2))

    // generate hash values for each token
    val sc = profiles.sparkContext
    val hashes = sc.broadcast(attributesPerToken.map {
      case (tokenID, _) =>
        val hashValues = getHashes(tokenID, numHashes)
        (tokenID, hashValues)
    }.collectAsMap())

    // (attribute, tokenIDs)
    val tokensPerAttribute: RDD[(String, Set[Int])] = attributesPerToken.flatMap {
      case (tokenID, attributes) =>
        attributes.map(attr => (attr, tokenID))
    }.groupByKey().map(x => (x._1, x._2.toSet))

    val allAttributes = tokensPerAttribute.map(_._1).collect()
    // transform hash values to attribute signature
    val attributeSignature: RDD[(String, Array[Int])] = tokensPerAttribute.map {
      case (attribute, tokenIDs) =>
        val signature = Array.fill[Int](numHashes) { Int.MaxValue }
        tokenIDs.foreach { tokenID =>
          val h = hashes.value(tokenID)
          for (i <- h.indices) {
            if (h(i) < signature(i)) {
              signature.update(i, h(i))
            }
          }
        }
        (attribute, signature)
    }
    hashes.unpersist()

    // band size: number of rows for each band
    val numRows = getNumRows(targetThreshold, numHashes)

    // transform signature to buckets for each attribute
    val attributeBuckets = attributeSignature.map {
      case (attribute, signature) =>
        // buckets: each bucket contains a band of rows, hashcode them to a integer
        val buckets = signature.sliding(numRows, numRows).map(_.toList.hashCode()).toIterable
        (attribute, buckets)
    }

    val bcAttributeSignature = sc.broadcast(attributeSignature.collectAsMap())
    // attributes per bucket
    val attributeClusters = attributeBuckets.flatMap { case (attribute, buckets) => buckets.map((_, attribute))}
      .groupByKey.map { case (bucket, attributes) => (bucket, attributes.toSet)}
      .filter { case (_, attributes) => attributes.size > 1 && attributes.size < 101 }
      .map(_._2).distinct()

    // generate edges with Jaccard Similarity
    val edges = attributeClusters.flatMap { cluster =>
      // enumerate all pairs of attributes
      cluster.toList.combinations(2).map(pair => {
        val head = pair.head
        val last = pair.last
        (head, (last, calcSimilarity(bcAttributeSignature.value(head), bcAttributeSignature.value(last))))
      })
    }

    bcAttributeSignature.unpersist()
    // produce all edges by swap two vertices
    val edgesPerAttr = edges.union(edges.map {
      case (attr1, (attr2, sim)) => (attr2, (attr1, sim))
    }).groupByKey().map(x => (x._1, x._2.toSet))
    // keep most similar neighbors for each attribute according to maxFactor
    val topEdges = edgesPerAttr.map {
      case (attribute, neighbors) =>
        val threshold = neighbors.map(_._2).max * maxFactor
        (attribute, neighbors.filter(_._2 >= threshold).map(_._1))
    }

    // call jgrapht locally due to limited attributes
    val graph = new SimpleGraph[String, DefaultEdge](classOf[DefaultEdge])
    val vertices = topEdges.map(_._1).union(topEdges.flatMap(_._2)).distinct().collect()
    vertices.foreach {
      v => graph.addVertex(v)
    }
    topEdges.foreach {
      case (from, to) =>
        to.foreach {
          t => graph.addEdge(from, t)
        }
    }

    val ci = new ConnectivityInspector(graph)
    val connectedComponent = ci.connectedSets()
    // attribute clusters as (attributes, clusterID)
    val clusters: Iterable[(Iterable[String], Int)] = (
      for (i <- 0 until connectedComponent.size()) yield {
        val iterator = connectedComponent.get(i).iterator()
        var attributes: List[String] = Nil
        while (iterator.hasNext) {
          attributes = iterator.next() :: attributes
        }
        (attributes, i)
      }).filter(_._1.nonEmpty)

    // default clusterID for non-clustered attributes
    val defaultClusterID = {
      if (clusters.isEmpty) {
        0
      } else {
        clusters.map(_._2).max + 1
      }
    }
    // all attributes including clustered and nonClustered attributes
    val clusteredAttributes = clusters.flatMap(_._1).toSet
    val nonClusteredAttributes = allAttributes.filter(!clusteredAttributes.contains(_))

    if (computeEntropy) {
      val clusterMap = clusters.flatMap {
        case (attributes, clusterID) =>
          attributes.map(attribute => (attribute, clusterID))
      }.toMap

      val normalizeEntropy = false
      val entropyPerAttribute = attributesToken.groupByKey().map {
        case (attribute, tokens) =>
          val totalTokens = tokens.size.toDouble
          val countOfTokens = tokens.groupBy(x => x).map(x => x._2.size)
          val tokensP = countOfTokens.map { count =>
              val p_i: Double = count.toDouble / totalTokens
              p_i * (Math.log10(p_i) / Math.log10(2.0d))
          }
          val entropy =
            if (normalizeEntropy) {
              -tokensP.sum / (Math.log10(totalTokens) / Math.log10(2.0d))
            } else {
              -tokensP.sum
            }
          (attribute, entropy)
      }
      attributesToken.unpersist()
      // cluster entropy = avg(attribute entropies)
      val entropyPerCluster = entropyPerAttribute.map {
        case (attribute, entropy) =>
          val clusterID = clusterMap.get(attribute)
          if (clusterID.isDefined) {
            (clusterID.get, entropy)
          } else {
            (defaultClusterID, entropy)
          }
      }.groupByKey().map(x => (x._1, x._2.sum / x._2.size))
      val entropyMap = entropyPerCluster.collectAsMap()
      val defaultEntropy = {
        val e = entropyMap.get(defaultClusterID)
        if (e.isDefined) {
          e.get
        } else {
          0.0
        }
      }
      clusters.map {
        case (attributes, clusterID) =>
          val entropy = {
            val e = entropyMap.get(clusterID)
            if (e.isDefined) {
              e.get
            } else {
              1.0
            }
          }
          AttributesCluster(clusterID, attributes.toList, entropy)
      }.toList ::: AttributesCluster(defaultClusterID, nonClusteredAttributes.toList, defaultEntropy) :: Nil
    } else {
      clusters.map {
        case (attributes, clusterID) =>
          AttributesCluster(clusterID, attributes.toList)
      }.toList ::: AttributesCluster(defaultClusterID, nonClusteredAttributes.toList) :: Nil
    }
  }

}
