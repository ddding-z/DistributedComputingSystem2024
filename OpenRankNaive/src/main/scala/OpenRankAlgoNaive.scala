import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object WeightedPageRank {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: WeightedPageRank <input-data-path> <output-rank-path>")
      System.exit(1)
    }

    val inputDataPath = args(0)
    val outputRankPath = args(1)

    val spark = SparkSession.builder
      .appName("WeightedPageRankNaive")
      .getOrCreate()

    val sc = spark.sparkContext

    val repos = spark.read.parquet(s"$inputDataPath/repos.parquet")
    val repoVertices: RDD[(VertexId, String)] = repos.rdd.map(row =>
      (row.getAs[Long]("repo_id"), row.getAs[String]("repo_name"))
    )

    val userRepoEdges = spark.read.parquet(s"$inputDataPath/user_repo_edges.parquet")
    val userRepoEdgesRDD: RDD[(Long, Long)] = userRepoEdges.rdd.map(row =>
      (row.getAs[Long]("src_id"), row.getAs[Long]("dst_id"))
    )

    val repoEdges: RDD[(Long, Long)] = userRepoEdgesRDD
      .groupByKey()
      .flatMap { case (userId, reposList) =>
        val repoPairs = reposList.toList.combinations(2)
        repoPairs.map {
          case Seq(repoA, repoB) => if (repoA < repoB) (repoA, repoB) else (repoB, repoA)
        }
      }

    val weightedEdges: RDD[Edge[Double]] = repoEdges
      .map { case (repoA, repoB) => ((repoA, repoB), 1) }
      .reduceByKey(_ + _)
      .map { case ((repoA, repoB), weight) =>
        Edge(repoA, repoB, weight.toDouble)
      }

    val undirectedEdges = weightedEdges.flatMap { edge =>
      Seq(
        Edge(edge.srcId, edge.dstId, edge.attr),
        Edge(edge.dstId, edge.srcId, edge.attr)
      )
    }

    val graph = Graph(repoVertices, undirectedEdges).cache()

    val dampingFactor = 0.85
    val tol = 1e-6
    var delta = Double.PositiveInfinity
    var iteration = 0
    val maxIterations = 20


    val outWeights = graph.aggregateMessages[Double](
      triplet => {
        triplet.sendToSrc(triplet.attr)
      },
      _ + _
    ).cache()

    val outWeightsMap = outWeights.collectAsMap()
    val outWeightsBroadcast = sc.broadcast(outWeightsMap)

    val numNodes = graph.numVertices.toDouble
    val teleport = (1.0 - dampingFactor) / numNodes

    var ranks: RDD[(VertexId, Double)] = repoVertices.map { case (id, _) => (id, 1.0) }.cache()

    while (delta > tol && iteration < maxIterations) {
      val ranksMap = ranks.collectAsMap()
      val ranksBroadcast = sc.broadcast(ranksMap)

      val contribs = graph.aggregateMessages[Double](
        triplet => {
          val rank = ranksBroadcast.value.getOrElse(triplet.srcId, 0.0)
          val totalWeight = outWeightsBroadcast.value.getOrElse(triplet.srcId, 1.0)
          val contribution = (rank * triplet.attr) / totalWeight
          triplet.sendToDst(contribution)
        },
        _ + _
      )

      val newRanks: RDD[(VertexId, Double)] = repoVertices.map { case (id, _) => (id, 0.0) }
        .leftOuterJoin(contribs)
        .mapValues { case (_, contribOpt) => teleport + dampingFactor * contribOpt.getOrElse(0.0) }

      val rankChanges = ranks.join(newRanks).map {
        case (id, (oldRank, newRank)) => math.abs(newRank - oldRank)
      }

      delta = rankChanges.reduce(math.max)
      println(s"Iteration $iteration: delta = $delta")

      ranks.unpersist()
      ranks = newRanks.cache()

      ranksBroadcast.unpersist()

      iteration += 1
    }

    val ranksByRepo = repoVertices.join(ranks).map {
      case (repoId, (repoName, rank)) => (repoName, rank)
    }

    val top10 = ranksByRepo.sortBy(_._2, ascending = false).take(10)
    println("Top 10 Repositories by PageRank:")
    top10.foreach { case (name, rank) => println(s"$name: $rank") }

    import spark.implicits._

    val ranksByRepoDF = ranksByRepo.toDF("repo_name", "page_rank")

    ranksByRepoDF.write
      .mode("overwrite")
      .parquet(s"$outputRankPath/page_rank_results.parquet")

    println(s"PageRank results have been saved to HDFS at: $outputRankPath")

    spark.stop()
  }
}
