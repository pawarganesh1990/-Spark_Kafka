package com.mastercard.spark.kafka
import java.util.Properties
import org.apache.kafka.clients.producer._
import scala.collection.Map

object ProducerData {
  def main(args: Array[String]) {
    val bootstrapServers = ":9092,localhost:9092"
    val groupId = "kafka-example"
    val topics = "order"
    /*    val maps =
    Map{
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> ":9092,localhost:9092";
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer";
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer";
      ProducerConfig.ACKS_CONFIG -> "all"
    }*/

    val props: Properties = {
      val p = new Properties()
      p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
      p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
      p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

      // optional configs
      // See the Kafka Producer tutorial for descriptions of the following
      p.put(ProducerConfig.ACKS_CONFIG, "all")
      //    p.put(ProducerConfig.RETRIES_CONFIG, 0)
      //    p.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384)
      //    p.put(ProducerConfig.LINGER_MS_CONFIG, 1)
      //    p.put(ProducerConfig.RETRIES_CONFIG, "TODO")
      //    p.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432)

      p
    }
    println("THis is data")
    val callback = new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        println("oh you crafty wily meta data you: " + metadata.toString)
      }
    }
    val producer = new KafkaProducer[String, String](props)
    for (k <- 1 to 10) {

      producer.send(new ProducerRecord(topics, s"key ${k}", "THis is Producer which is ready"))

      // with example callback
      //      producer.send(new ProducerRecord(topics,
      //                                  s"key ${k}",
      //                                 "oh the value!"),
      //                                        callback)
    }

    producer.close()

  }
}