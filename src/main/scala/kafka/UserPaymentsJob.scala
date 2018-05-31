package kafka

import java.io.IOException
import java.sql.Timestamp

import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


case class RecordBean(ts: String, userid: String, eventid: String)

object UserPaymentsJob extends App {
  val objectMapper = new ObjectMapper

  def jsonDecode(text: String): RecordBean = {
    try {
      objectMapper.readValue(text, classOf[RecordBean])
    } catch {
      case e: IOException => print(e.printStackTrace())
        null
    }
  }

  val sparkConf = new SparkConf().setAppName("User-Payment-Integration").setMaster("local[*]")
  val spark = SparkSession.builder.config(sparkConf).getOrCreate()
  val ssc = new StreamingContext(spark.sparkContext, Seconds(30))

  // Retrieving the details of the broker and the topic to connect
  val config = ConfigFactory.load()
  val topicName = config.getString("application.topic")
  val broker = config.getString("application.broker")

  // alias for simplicity
  type Record = ConsumerRecord[String, String]

  // publishing the schema type for further analysis
  val userSchema = StructType(Array(StructField("ts", StringType), StructField("userid", StringType), StructField("eventid", StringType)))

  val kafkaParams = Map[String, Object]("bootstrap.servers" -> broker, "key.deserializer" -> classOf[StringDeserializer], "value.deserializer" -> classOf[StringDeserializer], "group.id" -> "bookinggroup", "auto.offset.reset" -> "latest", "enable.auto.commit" -> (false: java.lang.Boolean))

  val topics = Array("bookings")

  val stream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

  val userEventsRDD:Unit = stream.foreachRDD((rdd: RDD[Record]) => {
    // convert string to PoJo and generate rows as tuple group
    val pairs = rdd.map(row => (row.timestamp(), jsonDecode(row.value()))).map(row => (row._2.userid, row._2.eventid))
    pairs.first()
  })

  ssc.start()
  ssc.awaitTermination()
}
