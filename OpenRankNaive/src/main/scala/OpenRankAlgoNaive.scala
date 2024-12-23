import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object WeightedPageRankRDD {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: WeightedPageRankRDD <input-data-path> <output-rank-path>")
      System.exit(1)
    }

    val inputDataPath = args(0)
    val outputRankPath = args(1)

    val conf = new SparkConf().setAppName("WeightedPageRankRDD")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()

    val repoVertices: RDD[(Long, (String, Double))] = spark.read.parquet(s"$inputDataPath/repos.parquet")
      .rdd 
      .map(row => (
        row.getAs[Long]("repo_id"),
        (row.getAs[String]("repo_name"), 1.0) 
      ))

    val userRepoEdgesRDD: RDD[(Long, Long)] = spark.read.parquet(s"$inputDataPath/user_repo_edges.parquet")
      .rdd
      .map(row => (
        row.getAs[Long]("src_id"),
        row.getAs[Long]("dst_id")
      ))

    val repoEdges: RDD[(Long, Long)] = userRepoEdgesRDD
      .groupByKey()
      .flatMap { case (_, reposList) =>
        reposList.toList.combinations(2).flatMap {
          case List(repoA, repoB) =>
            Seq((repoA, repoB), (repoB, repoA))
        }
      }

    val weightedEdges: RDD[((Long, Long), Double)] = repoEdges
      .map(edge => (edge, 1.0))
      .reduceByKey(_ + _)

    val edges: RDD[(Long, (Long, Double))] = weightedEdges.map {
      case ((src, dst), weight) => (src, (dst, weight))
    }

    val dampingFactor = 0.85
    val tol = 1e-6
    var delta = Double.PositiveInfinity
    var iteration = 0
    val maxIterations = 20

    val outWeights: RDD[(Long, Double)] = edges
      .map { case (src, (_, weight)) => (src, weight) }
      .reduceByKey(_ + _)

    val outWeightsMap = outWeights.collectAsMap()
    val outWeightsBroadcast = sc.broadcast(outWeightsMap)

    var ranks: RDD[(Long, Double)] = repoVertices.mapValues(_._2)

    while (delta > tol && iteration < maxIterations) {
      val contributions: RDD[(Long, Double)] = edges.join(ranks).flatMap {
        case (srcId, ((dstId, weight), rank)) =>
          val totalWeight = outWeightsBroadcast.value.getOrElse(srcId, 1.0)
          val contribution = (rank * weight) / totalWeight
          Seq((dstId, contribution))
      }

      val newRanks: RDD[(Long, Double)] = contributions
        .reduceByKey(_ + _)
        .mapValues(contrib => (1 - dampingFactor) + dampingFactor * contrib)

      val rankChanges: RDD[Double] = ranks.join(newRanks).map {
        case (_, (oldRank, newRank)) => math.abs(newRank - oldRank)
      }

      delta = rankChanges.max()
      println(s"Iteration $iteration: delta = $delta")

      ranks = newRanks
      iteration += 1
    }

    val ranksByRepo: RDD[(String, Double)] = repoVertices.join(ranks).map {
      case (_, ((repoName, _), rank)) => (repoName, rank)
    }

    // val top10 = ranksByRepo.sortBy(_._2, ascending = false).take(10)
    // println("Top 10 Repositories by PageRank:")
    // top10.foreach { case (name, rank) =>
    //   println(s"$name: $rank")
    // }

    import spark.implicits._

    val ranksByRepoDF = ranksByRepo.toDF("repo_name", "page_rank")
    ranksByRepoDF.write.mode("overwrite").parquet(s"$outputRankPath/page_rank_results.parquet")

    println(s"PageRank results have been saved to HDFS at: $outputRankPath")

    sc.stop()
  }
}
