package com.atguigu.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaCluster
import org.apache.spark.{SparkConf, streaming}

object LowKafka {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("LOWKAFKA")
    val ssc = new StreamingContext(conf,streaming.Seconds(3))

    val brokers = "hadoop102:9092,hadoop103:9092,hadoop104:9092"
    val topic = "first"
    val group = "bigdata"
    val deserialization ="org.apache.kafka.common.serialization.StringDeserializer"
    val kafkaParams = Map(
      "zookeeper.connect" -> "hadoop201:2181,hadoop202:2181,hadoop203:2181",
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
    )
    val kafkaCluster = new KafkaCluster(kafkaParams)
  }

  def getOffset()={

  }
}
