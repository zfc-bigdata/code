package com.atguigu.gmall0317.realtime.util


import java.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, Index, Search}
import org.elasticsearch.index.query.{BoolQueryBuilder, MatchQueryBuilder, RangeQueryBuilder}
import org.elasticsearch.search.builder.SearchSourceBuilder

object   MyEsUtil {
  var jestClientFactory :JestClientFactory=_

  def getJestClient():JestClient={
    if (jestClientFactory!=null) {
      jestClientFactory.getObject
    }else{
      val properties = PropertiesUtil.load("config.properties")
      val host = properties.getProperty("elasticsearch.host")
      val port = properties.getProperty("elasticsearch.port")

      jestClientFactory = new JestClientFactory
      jestClientFactory.setHttpClientConfig(new HttpClientConfig.Builder(
        "http://"+host+":"+port)
        .maxTotalConnection(4)
        .multiThreaded(true).build())
      jestClientFactory.getObject
    }
  }

  def saveDocBulk(docList:List[(Any,String)],indexName:String): Unit ={
    val jestClient = getJestClient
    val bulkBuilder = new Bulk.Builder
    bulkBuilder.defaultIndex(indexName)
      .defaultType("_doc")
    for((doc,id) <- docList){
      val index = new Index.Builder(doc).id(id).build()
      bulkBuilder.addAction(index)
    }
    val items = jestClient.execute(bulkBuilder.build()).getItems
    println("共提交:"+items.size()+"条数据")
    jestClient.close()
  }


  def saveDoc(doc:Any,indexName:String): Unit ={
    val jestClient = getJestClient
    val index = new Index.Builder(doc)
      .index(indexName).`type`("_doc").build()
    jestClient.execute(index)
    jestClient.close()
  }

  def searchDoc(indexName:String): Unit ={
    val jestClient = getJestClient()

    val sourceBuilder = new SearchSourceBuilder
    val bool = new BoolQueryBuilder
    bool.filter(new RangeQueryBuilder("doubanScore")
      .gte(6).lte(10))
    bool.must(new MatchQueryBuilder("name","red"))
    sourceBuilder.query(bool)
    println(sourceBuilder.toString)

    val search = new Search.Builder(sourceBuilder.toString)
      .addIndex(indexName).addType("movie").build()
    val searchResult = jestClient.execute(search)
    val hits = searchResult.getHits(classOf[util.Map[String, Any]])

    import collection.JavaConverters._
    for (hit <- hits.asScala){
      val source = hit.source
      println(source.get("name"))
      println(source.get("doubanScore"))
    }
    jestClient.close()
  }

  def main(args: Array[String]): Unit = {
    searchDoc("movie_index0317")
  }

  case class Movie(id:String,name:String)

}


