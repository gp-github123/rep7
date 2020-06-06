package www.huayan.com.day02

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class Day02ClassDemo {
  val sc = new SparkContext(new SparkConf().setMaster("local[6]").setAppName("map"))
  sc.setLogLevel("warn")

  @Test
  def map(): Unit = {

    sc.parallelize(Seq("Hello lily", "Hello lucy", "Hello tim"))
      .flatMap(_.split("\\s+"))
      .map((_, 1))
      .countByKey()
      .foreach(println(_))
  }

  @Test
  def filter(): Unit = {
    sc.parallelize(Seq(1, 2, 3, 4, 5, 6))
      .filter(_ >= 3)
      .foreach(println(_))
  }

  @Test
  def mapPartitions(): Unit = {
    sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      .mapPartitions(item => {
        item.map(da => (da._1, da._2 * 10))
      })
      .reduceByKey(_ + _)
      .foreach(println(_))
    sc.stop()
  }

  @Test
  def mapPartitionsWithIndex(): Unit = {
    sc.parallelize(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9))
      .mapPartitionsWithIndex((index, iter) => {
        iter.foreach(item => println(s"分区号：${index}\t数据“${item}"))
        iter
      }).collect()
  }

  @Test
  def filte1r(): Unit = {
    sc.parallelize(Seq("a", "c", "b", "d", "", "", ""))
      .filter(item => StringUtils.isNotBlank(item))
      .foreach(println(_))
  }

  @Test
  def sample(): Unit = {
    val rdd1 = sc.parallelize(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 7, 8, 9, 10, 7, 8, 9, 10, 7, 8, 9, 10, 7, 8, 9, 10, 7, 8, 9, 10, 7, 8, 9, 10, 7, 8, 9, 10, 7, 8, 9, 10, 7, 8, 9, 10))
    val rdd2: RDD[Int] = rdd1.sample(false, 0.1)
    rdd2.foreach(item => print(s"${item},"))
    val rdd3: RDD[(Int, Int)] = rdd2.map((_, 1))
    val rdd4: RDD[(Int, Int)] = rdd3.reduceByKey(_ + _)
    val rdd5: RDD[(Int, Int)] = rdd4.repartition(1)
    val rdd6: RDD[(Int, Int)] = rdd5.sortBy(_._1, false)
    rdd6.foreach(println(_))
  }

  @Test
  def mapValue(): Unit = {
    sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      .mapValues(_ * 10)
      .foreach(print(_))
  }

  @Test
  def intersection(): Unit = {
    val rdd1 = sc.parallelize(Seq(1, 2, 3, 4, 5))
    val rdd2 = sc.parallelize(Seq(3, 4, 5, 6, 7))
    val rdd3: RDD[Int] = rdd1.intersection(rdd2)
    rdd3.foreach(println(_))
  }

  @Test
  def union(): Unit = {
    val rdd1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 1)))
    val rdd2 = sc.parallelize(Seq(("a", 10), ("a", 11), ("a", 12)))
    val rdd3: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
    val rdd4: RDD[(String, Iterable[(Int, Int)])] = rdd3.groupByKey()
    rdd3.repartition(3).coalesce(3,true).map(item=>{
      (item._1,item._2._1+item._2._2)
    }).reduceByKey(_+_)
      .foreach(println(_))

     /* .flatMap(item => item._2).foreach(item => {
      val key = item._1
      val value = item._2
      println(key + value)
    })*/
  }

  @Test
  def combineBykey(): Unit = {
    val rdd: RDD[(String, Double)] = sc.parallelize(Seq(
      ("zhangsan", 99.0),
      ("zhangsan", 96.0),
      ("lisi", 97.0),
      ("lisi", 98.0),
      ("zhangsan", 97.0))
    )

    rdd.combineByKey(
      createCombiner = (curr: Double) => (curr, 1),
      mergeValue = (curr: (Double, Int), newValue: Double) => (curr._1 + newValue, curr._2 + 1),
      mergeCombiners = (curr: (Double, Int), agg: (Double, Int)) => (curr._1 + agg._1, curr._2 + agg._2)
    )
      .map(item => {
        (item._1, (item._2._1 / item._2._2).formatted("%.3f"))
      })
      .foreach(println(_))

  }
}
