package com.atguigu.test

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MyReceiverDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("wordcount")
    val scc = new StreamingContext(conf, Seconds(3))

    val dstream = scc.receiverStream(new MyReceiver("hadoop102",10000))

    val wordCountDStream = dstream.flatMap(_.split("\\W+")).map((_,1)).reduceByKey(_+_)

    wordCountDStream.print

    scc.start()

    scc.awaitTermination()
  }
}
