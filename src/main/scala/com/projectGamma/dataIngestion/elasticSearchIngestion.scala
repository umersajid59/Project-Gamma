package com.projectGamma.dataIngestion

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming
import org.elasticsearch.spark._
import java.sql.Timestamp
import scala.util.Try
import java.io.{File, PrintWriter}

object elasticSearchIngestion {
  def main(args : Array[String]){
    //creating log file
    val lgWrite = new PrintWriter(new File("consumerLog.log"))
    lgWrite.write("consumer log file\n")
    lgWrite.write(java.time.LocalDate.now.toString()+"\n")
    
    //creater logger for spark and kafka
    lgWrite.write(Logger.getLogger("org").setLevel(Level.ERROR).toString())
    lgWrite.write(Logger.getLogger("kafka").setLevel(Level.ALL).toString())
    
    
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("kafkaStreamTest")
      .config("es.index.auto.create", "true")
      .getOrCreate()
    
      
    //creating dummy dataframe for testing  
    import spark.implicits._
    val someDF = Seq(("LOLL",287.33,17,2, Timestamp.valueOf("2020-06-29 15:26:48"))).toDF("t", "p","x","s","dt")
    
    
    //crating connection to kafka and reading stream to dataframe
    val df = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "test-topic")
        .option("startingOffsets", "earliest") // From starting
        .load()
    
    //print schema of kafka stream
    df.printSchema()
    val x = java.time.LocalDate.now
    
    //extracting and transforming data from the stream  
    val data = df.select(get_json_object(($"value").cast("string"),"$.data.T").alias("ticker"),
                             get_json_object(($"value").cast("string"),"$.data.p").alias("price"),
                             get_json_object(($"value").cast("string"),"$.data.x").alias("exchange_id"),
                             get_json_object(($"value").cast("string"),"$.data.s").alias("trade_size"),
                             get_json_object(($"value").cast("string"),"$.data.t").alias("date_time"))
                             .na.drop()
                             .withColumn("date_time", ((col("date_time").cast("Long"))/1000000000).cast("timestamp"))
                             .withColumn("price",col("price").cast("Double"))
                             .withColumn("exchange_id",col("exchange_id").cast("Int"))
                             .withColumn("trade_size",col("trade_size").cast("Int"))
    
    //assertion test done on data to confirm correct datatype
    assert(someDF.schema("dt").dataType == data.schema("date_time").dataType)
    assert(someDF.schema("t").dataType == data.schema("ticker").dataType)
    assert(someDF.schema("p").dataType == data.schema("price").dataType)
    assert(someDF.schema("x").dataType == data.schema("exchange_id").dataType)
    assert(someDF.schema("s").dataType == data.schema("trade_size").dataType)                         
  
   if(data.isStreaming){
     lgWrite.write("data is streaming")
   }
   
   /*data.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()*/
   val d = data.filter(to_date(data("date_time")) === x.toString())
   .writeStream
   .format("org.elasticsearch.spark.sql")
   .outputMode("append")
   .option("es.nodes","localhost")
   .option("es.port","9200")
   .option("checkpointLocation","/tmp/")
   .option("es.resource", "index/type")
   .start()
   
   d.awaitTermination()
  }
}