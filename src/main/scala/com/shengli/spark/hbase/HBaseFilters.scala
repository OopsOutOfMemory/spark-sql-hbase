/*
* Copyright 2014 Sheng,Li
*/
package com.shengli.spark.hbase

import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.filter.{Filter => HBaseFilter}
import org.apache.spark.sql.sources.{Filter => SourceFilter}
import org.apache.hadoop.hbase.filter.CompareFilter._
import org.apache.spark.sql.sources._
import org.apache.hadoop.hbase.util.Bytes


object HBaseFilters  {

  def isRowKey(field: HBaseSchemaField) : Boolean = {
    val cfColArray = field.fieldName.split(":",-1)
    val cfName = cfColArray(0)
    val colName =  cfColArray(1)
    if(cfName=="" && colName=="key") true else false
  }

  def initialFilters(rawfilters: Array[SourceFilter], mapping : Map[HBaseSchemaField, RegisteredSchemaField] ) : Array[HBaseFilter] = {
    val reversedMapping = mapping.map(_ swap).map(kv=>(kv._1.fieldName,kv._2))
    val resultFilters = rawfilters.collect{
      case e @ EqualTo(attr, value) =>
        val hbaseField = reversedMapping.getOrElse(attr, sys.error("not found hbase table field in filter"))
        var filter:HBaseFilter = null
        //check if is row_key
        if(isRowKey(hbaseField)) {
          //TODO: rowkey filter
        }
        else {
          val cf_column = hbaseField.fieldName.split(":",-1)
          val cf = cf_column(0)
          val column = cf_column(1)
          filter = new SingleColumnValueFilter(
            Bytes.toBytes(cf),
            Bytes.toBytes(column),
            CompareOp.EQUAL,
            Bytes.toBytes(value.asInstanceOf[String])
          )
        }
        filter
    }
    resultFilters
  }
}