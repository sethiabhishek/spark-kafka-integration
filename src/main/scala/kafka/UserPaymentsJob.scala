package kafka

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read

object UserPaymentsJob extends App {

  case class RecordBean(ts: String, userid: String, eventid: String)

  implicit val formats = DefaultFormats

  val sparkConf = new SparkConf().setAppName("User-Payment-Integration").setMaster("local[*]")
  val spark = SparkSession.builder.config(sparkConf).getOrCreate()
  val ssc = new StreamingContext(spark.sparkContext, Seconds(60))

  // Retrieving the details of the broker and the topic to connect
  val config = ConfigFactory.load()
  val topicName = config.getString("application.topic")
  val broker = config.getString("application.broker")

  // alias for simplicity
  type Record = ConsumerRecord[String, String]

  val kafkaParams = Map[String, Object]("bootstrap.servers" -> broker, "key.deserializer" -> classOf[StringDeserializer], "value.deserializer" -> classOf[StringDeserializer], "group.id" -> "bookinggroup", "auto.offset.reset" -> "latest", "enable.auto.commit" -> (false: java.lang.Boolean))

  val topics = Array("bookings")

  val stream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

  val modified = stream.map(cons => read[RecordBean](cons.value())).map(record => (record.userid, record.eventid))
  modified.reduceByKey((r1, r2) => r1 + "," + r2).filter(!_._2.contains("SubmitPayment")).map(_._1).print()

  ssc.start()
  ssc.awaitTermination()
}
