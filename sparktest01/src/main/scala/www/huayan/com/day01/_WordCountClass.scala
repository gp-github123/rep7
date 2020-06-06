package www.huayan.com.day01

import org.apache.spark.{SparkConf, SparkContext}

object _WordCountClass {
  def main(args: Array[String]): Unit = {
    val sc=new SparkContext(new SparkConf().setAppName("wordcount").setMaster("local[6]"))
    sc.setLogLevel("warn")
    sc.textFile("dataset/wordcount.txt")
      .flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .foreach(println(_))
  }
  def add: Unit ={
    println("本地修改")
  }
}
