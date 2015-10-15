/**
 * Created by SSarka00c on 10/12/2015.
 */

import java.util.HashMap
import _root_.kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.api.java.{JavaRDD, JavaPairRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.{SparkContext, serializer, SparkConf}
import org.apache.kafka.common.serialization.Serializer._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import scala.Tuple2._

import scala.language.postfixOps

object streamTest {


  def main(args: Array[String]) {
    println("Hello, world!")

    import org.apache.log4j.Logger
    import org.apache.log4j.Level
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sConf = new SparkConf().setMaster("local[2]").setAppName("KStream") //.set("spark.driver.allowMultipleContexts","true")
    val ssc = new StreamingContext(sConf, Seconds(30))
    ssc.checkpoint("c:/checkpoint2")


    val groupId = "testStream"
    val topics = "topic_name"
    val numThreads = 1
    val topicMap = topics.split(",").map((_, numThreads)).toMap
    val brokers = "kafka-brkr-001h.com:9092"


    val storageLevel = StorageLevel.MEMORY_ONLY_SER

    topicMap.foreach(println(_))
    println("Brokers " + brokers)

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    val msgs = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    msgs.foreachRDD { rdd =>

      var ar = Array("Unknown" -> 1L)

      val offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsets.foreach { idx =>

        val topic = idx.topic
        val part = idx.partition
        val sIdx = idx.fromOffset
        val eIdx = idx.untilOffset

        println("Topic " + topic + ", Partition " + part + " Start Index " + sIdx + " End Index " + eIdx)
      }

      val x = rdd.collect().map { x =>
        val json = parse(x._2)
        val f1 = json.\\("EVT").\\("NAME").values.toString
        val f2 = json.\\("EVT").\\("VALUE").\\("TITLE").values.toString
        val f3 = json.\\("EVT").\\("VALUE").\\("STATUS").values.toString
        if(f1 == "tune" && f3 == "Fail")
          f2
        else
          "Unknown"

      }.map(title => (title,1L)).groupBy(_._1).map(x => println(x._1,x._2.size))

     

    }

    ssc.start()
    ssc.awaitTermination()
}

}
