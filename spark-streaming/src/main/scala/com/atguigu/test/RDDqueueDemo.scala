package com.atguigu.test

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{SparkConf, streaming}

import scala.collection.mutable

object RDDqueueDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDDQueueDemo").setMaster("local[*]")

    val scc: StreamingContext = new StreamingContext(conf, streaming.Seconds(5))

    val sc = scc.sparkContext

    val queue = mutable.Queue[RDD[Int]]()

    val rddDS = scc.queueStream(queue,false)

    rddDS.reduce(_+_).print

    scc.start()

    for(elem <- 1 to 5){
      queue += sc.parallelize(1 to 100)

      Thread.sleep(2000)
    }

    scc.awaitTermination()
  }

}
