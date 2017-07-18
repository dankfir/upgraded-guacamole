package main

import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import java.sql.{DriverManager, ResultSet}

import scala.util.parsing.json.{JSON, JSONArray, JSONFormat, JSONObject}
import java.sql.DriverManager
import java.sql.Connection
import java.util.Date


/**
  * Created by Dank on 2017/6/19.
  */



object Main {
//   Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("project2").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val ssc =new StreamingContext(sc,Seconds(1))
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "dank1,dank2,dank3")

    // Kafka configurations
    val topics = Set("topic3")
    //本地虚拟机ZK地址
    val brokers = "dank1:9092,dank2:9092,dank3:9092"

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder",
      "auto.offset.reset" -> "smallest")

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://dank1:3306/wind_analysis"
    val username = "root"
    val password = "root"
    var connection:Connection = null

    // Create a direct stream
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    val events = kafkaStream.flatMap(line => {
      Some(line._2)
    })
//    try {
//      Class.forName(driver)
//      connection = DriverManager.getConnection(url, username, password)
//      val statement = connection.createStatement()
//      val resultSet = statement.executeUpdate("select top 1 * from charactor order by change_date");
////      if(resultSet.)
//    } catch {
//      case e => e.printStackTrace
//    } finally {
//      connection.close()
//    }


    val Sourcedata = events.map(_.split(",")).filter(!_(0).equals("datasource"))
    val normalData=Sourcedata.filter(line=>{Check(line(4),line(22))})
    val abnormalData=Sourcedata.filter(line=>{!Check(line(4),line(22))})

    val norData=normalData.foreachRDD(rdd=>{
      rdd.foreachPartition(partitionOfRecord=>{
        partitionOfRecord.foreach(pair=>{
          val tableName="Normal"
          val hbaseConf = HBaseConfiguration.create()
          hbaseConf.set("hbase.zookeeper.quorum", "dank1,dank2,dank3")
          hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
          hbaseConf.set("hbase.defaults.for.version.skip", "true")
          //rowkey
          val rowkey = pair(2)+"_"+pair(1)
          //内容
          var tmp=""
          for(i<- 0 to pair.length-1){
            tmp += pair(i)+","
          }
          val value = tmp
          //组装数据
          val put = new Put(Bytes.toBytes(rowkey.toString))
          put.addColumn("cf".getBytes, "value".getBytes, Bytes.toBytes(value.toString))
          val StatTable = new HTable(hbaseConf, TableName.valueOf(tableName))
          StatTable.setAutoFlush(false, false)
          //写入数据缓存
          StatTable.setWriteBufferSize(10*1024*1024)
          StatTable.put(put)
          //提交
          StatTable.flushCommits()

        })
      })
    })

    val abnorData=abnormalData.foreachRDD(rdd=>{
      rdd.foreachPartition(partitionOfRecord=>{
        partitionOfRecord.foreach(pair=>{
          val tableName="Abnormal"
          val hbaseConf = HBaseConfiguration.create()
          hbaseConf.set("hbase.zookeeper.quorum", "dank1,dank2,dank3")
          hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
          hbaseConf.set("hbase.defaults.for.version.skip", "true")
          //rowkey
          val rowkey = pair(2)+"_"+pair(1)
          //内容
          var tmp=""
          for(i<- 0 to pair.length-1){
            tmp += pair(i)+","
          }
          val value = tmp
          //组装数据
          val put = new Put(Bytes.toBytes(rowkey.toString))
          put.addColumn("cf".getBytes, "value".getBytes, Bytes.toBytes(value.toString))
          val StatTable = new HTable(hbaseConf, TableName.valueOf(tableName))
          StatTable.setAutoFlush(false, false)
          //写入数据缓存
          StatTable.setWriteBufferSize(10*1024*1024)
          StatTable.put(put)
          //提交
          StatTable.flushCommits()

        })
      })
    })
    //高温报警

    val alertStream =
      events
      .window(Seconds(30), Seconds(5))
        .map(line=>line.split(","))
          .map(line=>{
        println(line(13))
        line
      })
      .filter(_ (13).toDouble > 20)
      val tempStream=alertStream
      .map(line => (line(1), 1))
      .reduceByKey((a: Int, b: Int) => a + b)
      .filter(_._2 >= 5)
        .map(line=>{
          try {
            println(line)
            import java.text.SimpleDateFormat
            val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            var time=df.format(new Date())
            println(time)
            Class.forName(driver)
            connection = DriverManager.getConnection(url, username, password)
            val statement = connection.createStatement()
            val resultSet = statement.executeUpdate("INSERT INTO warnni(alert_date,fan_no,des_info)" +
              " values('" + line._1 + "','" + time + "','" + line._2 + "')")
          } catch {
            case e => e.printStackTrace
          } finally {
            connection.close()
          }})
        .print()

    ssc.start()
    ssc.awaitTermination()


  }

  def Check(wind_speed :String,power:String):Boolean={
    var rs = true
    val wind=wind_speed.toDouble
    val p=power.toDouble
    if(wind_speed.equals("") || wind_speed.equals("-902") ||wind<3 ||wind >12
    || power.equals("") || power.equals("-902")|| p<(-0.5*1500) ||p> (1500*2)) rs =false
    rs
  }

}

