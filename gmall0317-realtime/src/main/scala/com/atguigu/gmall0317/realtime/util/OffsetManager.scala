package com.atguigu.gmall0317.realtime.util
import java.util
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange


object OffsetManager {

  def getOffset(topic:String,consumerGroupId:String): Map[TopicPartition, Long] ={
    val jedis = RedisUtil.getJedisClient
    val offsetKey = topic + ":" + consumerGroupId
    val offsetJavaMap =
      jedis.hgetAll(offsetKey)
    jedis.close()

    import collection.JavaConverters._

    if (offsetJavaMap!=null&&offsetJavaMap.size()>0) {

      val offsetList = offsetJavaMap.asScala.toList
      val offsetListTp = offsetList.map {
        case (partitionId, offset) =>
          println("加载-->分区: " + partitionId + "--偏移量: " + offset)
          (new TopicPartition(topic, partitionId.toInt), offset.toLong)
      }
      val map = offsetListTp.toMap
      map
    }else{
      println("没有找到已存在的偏移量!")
      null
    }
  }
  def saveOffset(topic:String,groupId:String,offsetRanges: Array[OffsetRange]): Unit ={
    val jedis = RedisUtil.getJedisClient
    val offsetKey = topic + ":" + groupId
    val offsetMap = new util.HashMap[String, String]()

    for(offsetRange <- offsetRanges){
      val partition = offsetRange.partition
      val offset = offsetRange.untilOffset
      offsetMap.put(partition.toString,offset.toString)
      println("写入偏移量-->分区: "+partition+"--偏移量"+offset)
    }
    jedis.hmset(offsetKey,offsetMap)
    jedis.close()

  }

}
