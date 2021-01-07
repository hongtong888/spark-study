package com.atguigu.test

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingWordCount2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","atguigu")

    val conf: SparkConf = new SparkConf().setAppName("wordcount2").setMaster("local[*]")

    val scc: StreamingContext = new StreamingContext(conf,Seconds(3))

    val lines: ReceiverInputDStream[String] = scc.socketTextStream("hadoop102",10000)

    val wordAndOne: DStream[(String, Int)] = lines.flatMap(_.split("\\W+")).map((_,1))

  }
  def updateFunction(newValue:Seq[Int],runningCount:Option[Int])={
    val newCount:Int = (0 /: newValue)(_+_)+runningCount.getOrElse(0)
    Some(newCount)
  }
}
