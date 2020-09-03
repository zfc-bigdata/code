package com.atguigu.gmall0317.realtime.ods

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0317.realtime.util.{MyKafkaSender, MyKafkaUtil, OffsetManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OdsGmallMaxwell {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("ods_gmall_maxwell_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topic = "ODS_DB_GMALL0317_M"
    val groupId = "ods_gmall_maxwell_group"

    var inputDstream : InputDStream[ConsumerRecord[String,String]] =null
    val offsetMap = OffsetManager.getOffset(topic, groupId)

    if (offsetMap!=null) {
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    }else{
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    var offsetRanges:Array[OffsetRange] =null
    val inputWithOffsetDstream = inputDstream.transform {
      rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
    }

    val jsonObjDstream = inputWithOffsetDstream.map {
      record =>
        val jsonString = record.value()
        JSON.parseObject(jsonString)
    }
    jsonObjDstream.foreachRDD{rdd =>
      rdd.foreach{
        jsonObj=>
          val dataJson = jsonObj.getString("data")
          val table = jsonObj.getString("table")
          val topic = "ODS_T_" + table.toUpperCase
          println(topic+"::"+dataJson)
          if(jsonObj.getString("type")!=null&&(
            (table=="order_info"&&jsonObj.getString("type")=="insert")
              || (table=="order_detail"&&jsonObj.getString("type")=="insert")
              || (table=="base_province"&&(jsonObj.getString("type")=="insert"||jsonObj.getString("type")=="update")||jsonObj.getString("type")=="bootstrap-insert")
              || (table=="user_info"&&(jsonObj.getString("type")=="insert"||jsonObj.getString("type")=="update")||jsonObj.getString("type")=="bootstrap-insert")
            )) {
            MyKafkaSender.send(topic,dataJson)
          }

      }
      OffsetManager.saveOffset(topic,groupId,offsetRanges)
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
