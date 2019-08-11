package com.mastercard.spark.kafka
import java.util.Collections
import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
object KafkaConsumer {
  val bootStrapServers = ":9092,localhost:9092"
  val groupId = "Spark Example"
  val topics = "order"

  val props: Properties = {
    val p = new Properties
    p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers)
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest") // default is latest
    p
  }

  def main(args: Array[String]): Unit = {

    val consumer = new KafkaConsumer[String, String](props)

    consumer.subscribe(Collections.singletonList(this.topics))

    while (true) {
      val records = consumer.poll(1000)
      import scala.collection.JavaConversions._
      val iterator = records.iterator();
      for (record <- iterator) {
        println("Message: (key: " +
          record.key() + ", with value: " + record.value() +
          ") at on partition " + record.partition() + " at offset " + record.offset())
      }

      // if you don't want to auto commit offset which is default
      // comment out below and adjust properties above
      /*
      try
        consumer.commitSync
      catch {
        case e: CommitFailedException =>
         // possible rollback of processed records
         // which may have failed to be delivered to ultimate destination
      }
      */
    }

    consumer.close()
  }
}