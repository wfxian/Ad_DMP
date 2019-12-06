package com.log

import com.util.{SchemaType, StrUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * 将数据格式转换为Parquet格式
  */
object log2Parquet {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("parquet")
      .master("local")
      //设置序列化方式
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    //判断路径
    if (args.length != 2) {
      println("目录不正确，退出")
      sys.exit()
    }

    //获取数据
    val Array(inputPath,outputPath) = args
    val lines = spark.sparkContext.textFile(inputPath)
    //先切分数据，按照逗号
    val rowRDD: RDD[Row] = lines.map(x => x.split(",", -1)).filter(_.length >= 85)
      .map(arr => {
        Row(
          arr(0),
          StrUtil.toInt(arr(1)),
          StrUtil.toInt(arr(2)),
          StrUtil.toInt(arr(3)),
          StrUtil.toInt(arr(4)),
          arr(5),
          arr(6),
          StrUtil.toInt(arr(7)),
          StrUtil.toInt(arr(8)),
          StrUtil.toDouble(arr(9)),
          StrUtil.toDouble(arr(10)),
          arr(11),
          arr(12),
          arr(13),
          arr(14),
          arr(15),
          arr(16),
          StrUtil.toInt(arr(17)),
          arr(18),
          arr(19),
          StrUtil.toInt(arr(20)),
          StrUtil.toInt(arr(21)),
          arr(22),
          arr(23),
          arr(24),
          arr(25),
          StrUtil.toInt(arr(26)),
          arr(27),
          StrUtil.toInt(arr(28)),
          arr(29),
          StrUtil.toInt(arr(30)),
          StrUtil.toInt(arr(31)),
          StrUtil.toInt(arr(32)),
          arr(33),
          StrUtil.toInt(arr(34)),
          StrUtil.toInt(arr(35)),
          StrUtil.toInt(arr(36)),
          arr(37),
          StrUtil.toInt(arr(38)),
          StrUtil.toInt(arr(39)),
          StrUtil.toDouble(arr(40)),
          StrUtil.toDouble(arr(41)),
          StrUtil.toInt(arr(42)),
          arr(43),
          StrUtil.toDouble(arr(44)),
          StrUtil.toDouble(arr(45)),
          arr(46),
          arr(47),
          arr(48),
          arr(49),
          arr(50),
          arr(51),
          arr(52),
          arr(53),
          arr(54),
          arr(55),
          arr(56),
          StrUtil.toInt(arr(57)),
          StrUtil.toDouble(arr(58)),
          StrUtil.toInt(arr(59)),
          StrUtil.toInt(arr(60)),
          arr(61),
          arr(62),
          arr(63),
          arr(64),
          arr(65),
          arr(66),
          arr(67),
          arr(68),
          arr(69),
          arr(70),
          arr(71),
          arr(72),
          StrUtil.toInt(arr(73)),
          StrUtil.toDouble(arr(74)),
          StrUtil.toDouble(arr(75)),
          StrUtil.toDouble(arr(76)),
          StrUtil.toDouble(arr(77)),
          StrUtil.toDouble(arr(78)),
          arr(79),
          arr(80),
          arr(81),
          arr(82),
          arr(83),
          StrUtil.toInt(arr(84))
        )
      })
    //构建DF
    val df: DataFrame = spark.createDataFrame(rowRDD,SchemaType.structType)

    //保存数据结果
    df.write.parquet(outputPath)
    //关闭
    spark.stop()
  }
}
