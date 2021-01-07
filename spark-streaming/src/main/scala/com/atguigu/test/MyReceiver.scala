package com.atguigu.test

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver



class MyReceiver(host:String,port:Int)extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  override def onStart(): Unit = {
    new Thread(){

      override def run(): Unit = {
        receive()
      }
    }.start()
  }

  def receive(): Unit = {
    val socket = new Socket(host,port)

    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream,StandardCharsets.UTF_8))

    var line = reader.readLine()
    while(line !=null){
      store(line)
      line= reader.readLine()
    }

    reader.close()
    socket.close()
    restart("retying")
  }

  override def onStop(): Unit = ???
}
