/*
* Copyright 2014 Sheng,Li
*/
package com.shengli.spark.hbase

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.RelationProvider
import com.shengli.spark.hbase


class DefaultSource extends RelationProvider {
  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    HBaseRelation(parameters)(sqlContext)
  }
}