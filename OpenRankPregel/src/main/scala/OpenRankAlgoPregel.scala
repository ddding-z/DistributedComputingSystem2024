import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object WeightedPageRankPregel {
  
  case class VertexAttr(pageRank: Double, sumWeights: Double)

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      System.err.println("Usage: WeightedPageRankPregel <input-data-path> <output-rank-path> <partition-strategy>")
      System.exit(1)
    }

    val inputDataPath = args(0)
    val outputRankPath = args(1)
    val partitionStrategyStr = args(2)

    val spark = SparkSession.builder
      .appName("WeightedPageRankPregel")
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
      .map { case (repoA, repoB) => ((repoA, repoB), 1.0) }
      .reduceByKey(_ + _)
      .map { case ((repoA, repoB), weight) =>
        Edge(repoA, repoB, weight)
      }
      .repartition(6)

    val undirectedEdges: RDD[Edge[Double]] = weightedEdges.flatMap { edge =>
      Seq(
        Edge(edge.srcId, edge.dstId, edge.attr),
        Edge(edge.dstId, edge.srcId, edge.attr)
      )
    }

    val numNodes = repoVertices.count().toDouble
    val initialPageRank = 1.0 / numNodes

    val initialVertices: RDD[(VertexId, VertexAttr)] = repoVertices.map { case (id, name) =>
      (id, VertexAttr(initialPageRank, 0.0))
    }.repartition(6)

    var graph: Graph[VertexAttr, Double] = Graph(initialVertices, undirectedEdges).cache()

    val partitionStrategy = partitionStrategyStr.toLowerCase match {
      case "randomvertexcut" => PartitionStrategy.RandomVertexCut
      case "canonicalrandomvertexcut" => PartitionStrategy.CanonicalRandomVertexCut
      case "edgepartition1d" => PartitionStrategy.EdgePartition1D
      case "edgepartition2d" => PartitionStrategy.EdgePartition2D
      case _ =>
        System.err.println(s"Unknown partition strategy: $partitionStrategyStr")
        System.exit(1)
        PartitionStrategy.RandomVertexCut
    }

    graph = graph.partitionBy(partitionStrategy).cache()

    val dampingFactor = 0.85
    val tol = 1e-6
    val maxIterations = 20

    val teleport = (1.0 - dampingFactor) / numNodes
    
    val sumWeights: VertexRDD[Double] = graph.aggregateMessages[Double](
      triplet => {
        triplet.sendToSrc(triplet.attr)
      },
      _ + _
    )

    val graphWithSumWeights: Graph[VertexAttr, Double] = graph.joinVertices(sumWeights) { case (id, attr, sum) =>
      attr.copy(sumWeights = sum)
    }.cache()

    val pageRankGraph: Graph[VertexAttr, Double] = graphWithSumWeights.pregel[Double](
      initialMsg = 0.0,
      maxIterations = maxIterations,
      activeDirection = EdgeDirection.Out
    )(
      (id, attr, msgSum) => {
        val newPageRank = teleport + dampingFactor * msgSum
        attr.copy(pageRank = newPageRank)
      },

      triplet => {
        val srcSumWeights = triplet.srcAttr.sumWeights
        if (srcSumWeights > 0) {
          val contribution = triplet.srcAttr.pageRank * triplet.attr / srcSumWeights
          Iterator((triplet.dstId, contribution))
        } else {
          Iterator.empty
        }
      },

      (a, b) => a + b
    ).cache()

    val ranks: RDD[(VertexId, Double)] = pageRankGraph.vertices.map { case (id, attr) =>
      (id, attr.pageRank)
    }
    val repoNamesWithRanks: RDD[(String, Double)] = repoVertices.join(ranks).map {
      case (repoId, (repoName, rank)) => (repoName, rank)
    }

    import spark.implicits._

    val ranksByRepoDF = repoNamesWithRanks.toDF("repo_name", "page_rank")

    ranksByRepoDF.write
      .mode("overwrite")
      .parquet(s"$outputRankPath/page_rank_results_pregel.parquet")

    println(s"PageRank (Pregel) results have been saved to HDFS at: $outputRankPath")

    spark.stop()
  }
}

