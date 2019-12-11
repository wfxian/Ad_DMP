package com.graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 图计算例子
  */
object GraphxTest {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("Graphx").master("local[*]").getOrCreate()

    //构建点和边
    //构建点
    val vdRdd: RDD[(Long, (String, Int))] = spark.sparkContext.makeRDD(Seq(
      (1L, ("科比", 24)),
      (2L, ("詹姆斯", 23)),
      (6L, ("保罗", 3)),
      (9L, ("乔治", 13)),
      (133L, ("欧文", 11)),
      (5L, ("莱昂纳德", 2)),
      (7L, ("字母哥", 34)),
      (158L, ("东契奇", 77)),
      (16L, ("杜兰特", 35)),
      (138L, ("威斯布鲁克", 0)),
      (21L, ("麦蒂", 1)),
      (44L, ("安东尼", 7))
    ))

    //构建边
    val edgeRdd: RDD[Edge[Int]] = spark.sparkContext.makeRDD(Seq(
      Edge(1L, 133L, 0),
      Edge(2L, 133L, 0),
      Edge(6L, 133L, 0),
      Edge(9L, 133L, 0),
      Edge(16L, 138L, 0),
      Edge(6L, 138L, 0),
      Edge(21L, 138L, 0),
      Edge(44L, 138L, 0),
      Edge(1L, 158L, 0),
      Edge(1L, 158L, 0)
    ))


    //构建图
    val graph = Graph(vdRdd,edgeRdd)
    //取出所有连接的顶点
    val vd: VertexRDD[VertexId] = graph.connectedComponents().vertices
    //整理数据
    vd.join(vdRdd).map{
      case (userId,(vd,(name,age))) => (vd,List((name,age)))
    }.reduceByKey(_++_).foreach(println)
  }

}
