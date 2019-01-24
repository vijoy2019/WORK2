package com.intuit.matrics

import java.util.HashMap

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._




object KafkaWordCount {

  //def main(args: Array[String]): Unit = {
  def main(args: Array[String]) {


    val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaReceiver")
    val ssc = new StreamingContext(conf, Seconds(2))
    val kafkaStream = KafkaUtils.createStream(ssc, "localhost:2181","spark-streaming-consumer-group", Map("test" -> 1))
    kafkaStream.print()


    ssc.start()
    ssc.awaitTermination()
    //ssc.stop()


  }
}
