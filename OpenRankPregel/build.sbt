name := "OpenRankPregel"

version := "1.0"

scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.3",        // Spark Cor
  "org.apache.spark" %% "spark-sql" % "3.5.3",         // Spark SQL
  "org.apache.spark" %% "spark-graphx" % "3.5.3",      // Spark GraphX (用于图计算)
  "org.apache.hadoop" % "hadoop-client" % "3.2.1"       // Hadoop 客户端
)
