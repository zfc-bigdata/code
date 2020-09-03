package com.atguigu.gmall0317.realtime.dim

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0317.realtime.bean.{UserInfo}
import com.atguigu.gmall0317.realtime.util.{MyKafkaUtil, OffsetManager}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.phoenix.spark._

object DimUserlnfoApp {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("dim_user_info_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topic = "ODS_T_BASE_USER"
    val groupId = "dim_user_info_group"

    var inputDstream : InputDStream[ConsumerRecord[String,String]] =null
    val offsetMap = OffsetManager.getOffset(topic, groupId)

    if (offsetMap!=null) {
      inputDstream = MyKafkaUtil
        .getKafkaStream(topic, ssc, offsetMap, groupId)
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
      rdd=>
        val userRDD = rdd.map {
          jsonObj =>
            val userInfo = UserInfo(jsonObj.getString("id"),
              jsonObj.getString("log_name"),
              jsonObj.getString("nick_name"),
              jsonObj.getString("passwd"),
              jsonObj.getString("name"),
              jsonObj.getString("phone_num"),
              jsonObj.getString("email"),
              jsonObj.getString("head_img"),
              jsonObj.getString("user_level"),
              jsonObj.getString("birthday"),
              jsonObj.getString("gender"),
              jsonObj.getString("create_time"),
              jsonObj.getString("operate_time")
            )
            println(userInfo)
            userInfo
        }
        userRDD.saveToPhoenix("GMALL0317_USER_INFO",
          Seq("ID","LOG_NAME","NICK_NAME","PASSWD","NAME","PHONE_NUM","EMAIL",
            "HEAD_IMG","USER_LEVEL","BIRTHDAY","GENDER","CREATE_TIME","OPERATE_TIME"),
          new Configuration,Some("hadoop102,hadoop103,hadoop104:2181"))

        OffsetManager.saveOffset(topic,groupId,offsetRanges)

    }

    ssc.start()
    ssc.awaitTermination()
  }

}
