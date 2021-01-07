package com.atguigu.sparkcore.day03.transformation

import org.apache.spark.{SparkConf, SparkContext}

object ValueTransform {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
    val sc = new SparkContext(conf)

    map(sc)
    mapPartitions(sc)
    mapPartitionsWithIndex(sc)
    flatMap(sc)
    glom(sc)
    groupBy(sc)
    filter(sc)
    distince(sc)
    coalesce(sc)
    repartition(sc)
    sortBy(sc)

  }

  def map(sc: SparkContext): Unit = {
    val source = sc.parallelize(1 to 10)
    val result = source.map(_*2)
    println(result.collect().toList)
  }

  def mapPartitions(sc: SparkContext) = {
    val source = sc.parallelize(Array(10,20,30,40,50,60))
    val result = source.mapPartitions(items => items.map(_*2))
    println(result.collect().toList)
  }

  def mapPartitionsWithIndex(sc: SparkContext): Unit = {
    val source = sc.parallelize(Array(10,20,30,40,50,60))
    val result = source.mapPartitionsWithIndex((index,items) => items.map((index,_)))
    println(result.collect().toList)

  }

  def flatMap(sc: SparkContext): Unit = {
    val source = sc.parallelize(Array(1,2,3,4,5))
//    val result = source.flatMap(item => Array(item * item))
    val result = source.flatMap(1 to _)
    println(result.collect().toList)
  }

  def glom(sc: SparkContext): Unit = {
    val source = sc.parallelize(1 to 16,4)
    val result = source.glom()
    val list = result.collect().toList
    list.foreach(item =>{
      println(item.toList)
    })
  }

  //按键分组
  def groupBy(sc: SparkContext): Unit = {
    val source = sc.parallelize(1 to 4)
    val result = source.groupBy(_%2)
    println(result.collect())
  }

  //过滤操作
  def filter(sc: SparkContext): Unit = {
    val source = sc.parallelize(Array("xiaoli", "laoli", "laowang", "xiaocang", "xiaojing", "xiaokong"))
    val result = source.filter(_.contains("xiao"))
    println(result.collect().toList)
  }

  //去重操作
  def distince(sc: SparkContext): Unit = {
    val source = sc.parallelize(Array(10,10,2,5,3,5,3,6,9,1))
    val result = source.distinct(2)
    println(result.collect().toList)
  }

  //降分区 可以传入参数自定义是否进行shuffle
  def coalesce(sc: SparkContext): Unit = {
    val source = sc.parallelize(1 to 16,4)
    println(source.partitions.size)

    val result = source.coalesce(2)
    println(result.partitions.size)
  }

  //重新进行分区，会重新进行 shuffle，底层调用的coalesce
  def repartition(sc: SparkContext): Unit = {
    val source = sc.parallelize(1 to 16,4)
    val result = source.repartition(2)
    println(result.partitions.size)
  }

  def sortBy(sc: SparkContext): Unit = {
    val source = sc.parallelize(List(2,1,3,4))
    val result = source.sortBy(x => x,false)
    println(result.collect().toList)
  }
}
