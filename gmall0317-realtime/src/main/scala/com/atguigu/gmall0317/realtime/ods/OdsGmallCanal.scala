package com.atguigu.gmall0317.realtime.ods

import com.atguigu.gmall0317.realtime.util.{MyKafkaSender, MyKafkaUtil, OffsetManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.alibaba.fastjson.JSON

object OdsGmallCanal {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("ods_gmall_canal_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topic = "ODS_DB_GMALL0317_C"
    val groupId = "ods_gmall_canal_group"

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
    jsonObjDstream.foreachRDD{
      rdd =>
        rdd.foreach{
          jsonObj=>
            val jSONArray = jsonObj.getJSONArray("data")
            val table = jsonObj.getString("table")
            val topic = "ODS_T_" + table.toUpperCase
            for(i<- 0 to jSONArray.size()-1){
              val dataJson = jSONArray.getString(i)
              MyKafkaSender.send(topic,dataJson)
            }
        }
        OffsetManager.saveOffset(topic,groupId,offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()

  }
}