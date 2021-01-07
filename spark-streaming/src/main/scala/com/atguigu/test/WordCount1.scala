package com.atguigu.test

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")

    val scc: StreamingContext = new StreamingContext(conf, Seconds(5))

    val dstream: ReceiverInputDStream[String] = scc.socketTextStream("hadoop102",10000)

    val wordCountStream: DStream[(String, Int)] = dstream.flatMap(_.split("\\W+")).map((_,1)).reduceByKey(_+_)

    wordCountStream.print

    scc.start()

    scc.awaitTermination()
  }

}
