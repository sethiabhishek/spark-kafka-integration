package kafka

import java.sql.Timestamp

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UserPaymentsJob extends App {

  val sparkConf = new SparkConf().setAppName("User-Payment-Integration")
  val ssc = new StreamingContext(sparkConf, Seconds(2))
  val spark = SparkSession.builder.appName("User-Payment-Integration").master("local").getOrCreate()

  import spark.implicits._

  // Retrieving the details of the broker and the topic to connect
  val config = ConfigFactory.load()
  val topicName = config.getString("application.topic")
  val broker = config.getString("application.broker")


  // connect to the kafka topic, mentioning local broker for now
  val df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", broker).option("subscribe", topicName).load()

  df.printSchema()

  // define the schema for the retrieval
  val messageSchema = StructType(Array(StructField("ts", StringType), StructField("userid", StringType), StructField("eventid", StringType)))

}
